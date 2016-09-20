/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"net"
	"net/url"
	"path"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
)

// Global event notification queue. This is the queue that would be used to send all notifications.
type eventNotifier struct {
	rwMutex *sync.RWMutex

	// Collection of 'bucket' and notification config.
	notificationConfigs map[string]*notificationConfig
	snsTargets          map[string][]chan []NotificationEvent
	queueTargets        map[string]*logrus.Logger
}

// Represents data to be sent with notification event.
type eventData struct {
	Type      EventName
	Bucket    string
	ObjInfo   ObjectInfo
	ReqParams map[string]string
}

// New notification event constructs a new notification event message from
// input request metadata which completed successfully.
func newNotificationEvent(event eventData) NotificationEvent {
	/// Construct a new object created event.
	region := serverConfig.GetRegion()
	tnow := time.Now().UTC()
	sequencer := fmt.Sprintf("%X", tnow.UnixNano())
	// Following blocks fills in all the necessary details of s3 event message structure.
	// http://docs.aws.amazon.com/AmazonS3/latest/dev/notification-content-structure.html
	nEvent := NotificationEvent{
		EventVersion:      "2.0",
		EventSource:       "aws:s3",
		AwsRegion:         region,
		EventTime:         tnow.Format(timeFormatAMZ),
		EventName:         event.Type.String(),
		UserIdentity:      defaultIdentity(),
		RequestParameters: event.ReqParams,
		ResponseElements:  map[string]string{},
		S3: eventMeta{
			SchemaVersion:   "1.0",
			ConfigurationID: "Config",
			Bucket: bucketMeta{
				Name:          event.Bucket,
				OwnerIdentity: defaultIdentity(),
				ARN:           "arn:aws:s3:::" + event.Bucket,
			},
		},
	}

	escapedObj := url.QueryEscape(event.ObjInfo.Name)
	// For delete object event type, we do not need to set ETag and Size.
	if event.Type == ObjectRemovedDelete {
		nEvent.S3.Object = objectMeta{
			Key:       escapedObj,
			Sequencer: sequencer,
		}
		return nEvent
	}
	// For all other events we should set ETag and Size.
	nEvent.S3.Object = objectMeta{
		Key:       escapedObj,
		ETag:      event.ObjInfo.MD5Sum,
		Size:      event.ObjInfo.Size,
		Sequencer: sequencer,
	}
	// Success.
	return nEvent
}

// Fetch the saved queue target.
func (en eventNotifier) GetQueueTarget(queueARN string) *logrus.Logger {
	return en.queueTargets[queueARN]
}

func (en eventNotifier) GetSNSTarget(snsARN string) []chan []NotificationEvent {
	en.rwMutex.RLock()
	defer en.rwMutex.RUnlock()
	return en.snsTargets[snsARN]
}

// Set a new sns target for an input sns ARN.
func (en *eventNotifier) SetSNSTarget(snsARN string, listenerCh chan []NotificationEvent) error {
	en.rwMutex.Lock()
	defer en.rwMutex.Unlock()
	if listenerCh == nil {
		return errInvalidArgument
	}
	en.snsTargets[snsARN] = append(en.snsTargets[snsARN], listenerCh)
	return nil
}

// Remove sns target for an input sns ARN.
func (en *eventNotifier) RemoveSNSTarget(snsARN string, listenerCh chan []NotificationEvent) {
	en.rwMutex.Lock()
	defer en.rwMutex.Unlock()
	snsTarget, ok := en.snsTargets[snsARN]
	if ok {
		for i, savedListenerCh := range snsTarget {
			if listenerCh == savedListenerCh {
				snsTarget = append(snsTarget[:i], snsTarget[i+1:]...)
				if len(snsTarget) == 0 {
					delete(en.snsTargets, snsARN)
					break
				}
				en.snsTargets[snsARN] = snsTarget
			}
		}
	}
}

// Returns true if bucket notification is set for the bucket, false otherwise.
func (en *eventNotifier) IsBucketNotificationSet(bucket string) bool {
	if en == nil {
		return false
	}
	en.rwMutex.RLock()
	defer en.rwMutex.RUnlock()
	_, ok := en.notificationConfigs[bucket]
	return ok
}

// Fetch bucket notification config for an input bucket.
func (en eventNotifier) GetBucketNotificationConfig(bucket string) *notificationConfig {
	en.rwMutex.RLock()
	defer en.rwMutex.RUnlock()
	return en.notificationConfigs[bucket]
}

// Set a new notification config for a bucket, this operation will overwrite any previous
// notification configs for the bucket.
func (en *eventNotifier) SetBucketNotificationConfig(bucket string, notificationCfg *notificationConfig) error {
	en.rwMutex.Lock()
	defer en.rwMutex.Unlock()
	if notificationCfg == nil {
		return errInvalidArgument
	}
	en.notificationConfigs[bucket] = notificationCfg
	return nil
}

// eventNotify notifies an event to relevant targets based on their
// bucket notification configs.
func eventNotify(event eventData) {
	// Notifies a new event.
	// List of events reported through this function are
	//  - s3:ObjectCreated:Put
	//  - s3:ObjectCreated:Post
	//  - s3:ObjectCreated:Copy
	//  - s3:ObjectCreated:CompleteMultipartUpload
	//  - s3:ObjectRemoved:Delete

	nConfig := globalEventNotifier.GetBucketNotificationConfig(event.Bucket)
	// No bucket notifications enabled, drop the event notification.
	if nConfig == nil {
		return
	}
	if len(nConfig.QueueConfigs) == 0 && len(nConfig.TopicConfigs) == 0 && len(nConfig.LambdaConfigs) == 0 {
		return
	}

	// Event type.
	eventType := event.Type.String()

	// Object name.
	objectName := event.ObjInfo.Name

	// Save the notification event to be sent.
	notificationEvent := []NotificationEvent{newNotificationEvent(event)}

	// Validate if the event and object match the queue configs.
	for _, qConfig := range nConfig.QueueConfigs {
		eventMatch := eventMatch(eventType, qConfig.Events)
		ruleMatch := filterRuleMatch(objectName, qConfig.Filter.Key.FilterRules)
		if eventMatch && ruleMatch {
			targetLog := globalEventNotifier.GetQueueTarget(qConfig.QueueARN)
			if targetLog != nil {
				targetLog.WithFields(logrus.Fields{
					"Key":       path.Join(event.Bucket, objectName),
					"EventType": eventType,
					"Records":   notificationEvent,
				}).Info()
			}
		}
	}
	// Validate if the event and object match the sns configs.
	for _, topicConfig := range nConfig.TopicConfigs {
		ruleMatch := filterRuleMatch(objectName, topicConfig.Filter.Key.FilterRules)
		eventMatch := eventMatch(eventType, topicConfig.Events)
		if eventMatch && ruleMatch {
			targetListeners := globalEventNotifier.GetSNSTarget(topicConfig.TopicARN)
			for _, listener := range targetListeners {
				listener <- notificationEvent
			}
		}
	}
}

// loads notifcation config if any for a given bucket, returns back structured notification config.
func loadNotificationConfig(bucket string, objAPI ObjectLayer) (*notificationConfig, error) {
	// Construct the notification config path.
	notificationConfigPath := path.Join(bucketConfigPrefix, bucket, bucketNotificationConfig)
	objInfo, err := objAPI.GetObjectInfo(minioMetaBucket, notificationConfigPath)
	err = errorCause(err)
	if err != nil {
		// 'notification.xml' not found return 'errNoSuchNotifications'.
		// This is default when no bucket notifications are found on the bucket.
		switch err.(type) {
		case ObjectNotFound:
			return nil, errNoSuchNotifications
		}
		errorIf(err, "Unable to load bucket-notification for bucket %s", bucket)
		// Returns error for other errors.
		return nil, err
	}
	var buffer bytes.Buffer
	err = objAPI.GetObject(minioMetaBucket, notificationConfigPath, 0, objInfo.Size, &buffer)
	err = errorCause(err)
	if err != nil {
		// 'notification.xml' not found return 'errNoSuchNotifications'.
		// This is default when no bucket notifications are found on the bucket.
		switch err.(type) {
		case ObjectNotFound:
			return nil, errNoSuchNotifications
		}
		errorIf(err, "Unable to load bucket-notification for bucket %s", bucket)
		// Returns error for other errors.
		return nil, err
	}

	// Unmarshal notification bytes.
	notificationConfigBytes := buffer.Bytes()
	notificationCfg := &notificationConfig{}
	if err = xml.Unmarshal(notificationConfigBytes, &notificationCfg); err != nil {
		return nil, err
	} // Successfully marshalled notification configuration.

	// Return success.
	return notificationCfg, nil
}

// loads all bucket notifications if present.
func loadAllBucketNotifications(objAPI ObjectLayer) (map[string]*notificationConfig, error) {
	// List buckets to proceed loading all notification configuration.
	buckets, err := objAPI.ListBuckets()
	if err != nil {
		return nil, err
	}

	configs := make(map[string]*notificationConfig)

	// Loads all bucket notifications.
	for _, bucket := range buckets {
		nCfg, nErr := loadNotificationConfig(bucket.Name, objAPI)
		if nErr != nil {
			if nErr == errNoSuchNotifications {
				continue
			}
			return nil, nErr
		}
		configs[bucket.Name] = nCfg
	}

	// Success.
	return configs, nil
}

// Loads all queue targets, initializes each queueARNs depending on their config.
// Each instance of queueARN registers its own logrus to communicate with the
// queue service. QueueARN once initialized is not initialized again for the
// same queueARN, instead previous connection is used.
func loadAllQueueTargets() (map[string]*logrus.Logger, error) {
	queueTargets := make(map[string]*logrus.Logger)
	// Load all amqp targets, initialize their respective loggers.
	for accountID, amqpN := range serverConfig.GetAMQP() {
		if !amqpN.Enable {
			continue
		}
		// Construct the queue ARN for AMQP.
		queueARN := minioSqs + serverConfig.GetRegion() + ":" + accountID + ":" + queueTypeAMQP
		// Queue target if already initialized we move to the next ARN.
		_, ok := queueTargets[queueARN]
		if ok {
			continue
		}
		// Using accountID we can now initialize a new AMQP logrus instance.
		amqpLog, err := newAMQPNotify(accountID)
		if err != nil {
			// Encapsulate network error to be more informative.
			if _, ok := err.(net.Error); ok {
				return nil, &net.OpError{
					Op:  "Connecting to " + queueARN,
					Net: "tcp",
					Err: err,
				}
			}
			return nil, err
		}
		queueTargets[queueARN] = amqpLog
	}
	// Load redis targets, initialize their respective loggers.
	for accountID, redisN := range serverConfig.GetRedis() {
		if !redisN.Enable {
			continue
		}
		// Construct the queue ARN for Redis.
		queueARN := minioSqs + serverConfig.GetRegion() + ":" + accountID + ":" + queueTypeRedis
		// Queue target if already initialized we move to the next ARN.
		_, ok := queueTargets[queueARN]
		if ok {
			continue
		}
		// Using accountID we can now initialize a new Redis logrus instance.
		redisLog, err := newRedisNotify(accountID)
		if err != nil {
			// Encapsulate network error to be more informative.
			if _, ok := err.(net.Error); ok {
				return nil, &net.OpError{
					Op:  "Connecting to " + queueARN,
					Net: "tcp",
					Err: err,
				}
			}
			return nil, err
		}
		queueTargets[queueARN] = redisLog
	}
	// Load elastic targets, initialize their respective loggers.
	for accountID, elasticN := range serverConfig.GetElasticSearch() {
		if !elasticN.Enable {
			continue
		}
		// Construct the queue ARN for Elastic.
		queueARN := minioSqs + serverConfig.GetRegion() + ":" + accountID + ":" + queueTypeElastic
		_, ok := queueTargets[queueARN]
		if ok {
			continue
		}
		// Using accountID we can now initialize a new ElasticSearch logrus instance.
		elasticLog, err := newElasticNotify(accountID)
		if err != nil {
			// Encapsulate network error to be more informative.
			if _, ok := err.(net.Error); ok {
				return nil, &net.OpError{
					Op: "Connecting to " + queueARN, Net: "tcp",
					Err: err,
				}
			}
			return nil, err
		}
		queueTargets[queueARN] = elasticLog
	}
	// Successfully initialized queue targets.
	return queueTargets, nil
}

// Global instance of event notification queue.
var globalEventNotifier *eventNotifier

// Initialize event notifier.
func initEventNotifier(objAPI ObjectLayer) error {
	if objAPI == nil {
		return errInvalidArgument
	}

	// Read all saved bucket notifications.
	configs, err := loadAllBucketNotifications(objAPI)
	if err != nil {
		return err
	}

	// Initializes all queue targets.
	queueTargets, err := loadAllQueueTargets()
	if err != nil {
		return err
	}

	// Inititalize event notifier queue.
	globalEventNotifier = &eventNotifier{
		rwMutex:             &sync.RWMutex{},
		notificationConfigs: configs,
		queueTargets:        queueTargets,
		snsTargets:          make(map[string][]chan []NotificationEvent),
	}

	return nil
}
