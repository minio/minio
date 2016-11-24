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
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net"
	"net/url"
	"path"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
)

type externalNotifier struct {
	// Per-bucket notification config. This is updated via
	// PutBucketNotification API.
	notificationConfigs map[string]*notificationConfig

	// An external target keeps a connection to an external
	// service to which events are to be sent. It is a mapping
	// from an ARN to a log object
	targets map[string]*logrus.Logger

	rwMutex *sync.RWMutex
}

type internalNotifier struct {
	// per-bucket listener configuration. This is updated
	// when listeners connect or disconnect.
	listenerConfigs map[string][]listenerConfig

	// An internal target is a peer Minio server, that is
	// connected to a listening client. Here, targets is a map of
	// listener ARN to log object.
	targets map[string]*listenerLogger

	// Connected listeners is a map of listener ARNs to channels
	// on which the ListenBucket API handler go routine is waiting
	// for events to send to a client.
	connectedListeners map[string]chan []NotificationEvent

	rwMutex *sync.RWMutex
}

// Global event notification configuration. This structure has state
// about configured external notifications, and run-time configuration
// for listener notifications.
type eventNotifier struct {

	// `external` here refers to notification configuration to
	// send events to supported external systems
	external externalNotifier

	// `internal` refers to notification configuration for live
	// listening clients. Events for a client are send from all
	// servers, internally to a particular server that is
	// connected to the client.
	internal internalNotifier
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
	// Following blocks fills in all the necessary details of s3
	// event message structure.
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

// Fetch the external target. No locking needed here since this map is
// never written after initial startup.
func (en eventNotifier) GetExternalTarget(queueARN string) *logrus.Logger {
	return en.external.targets[queueARN]
}

func (en eventNotifier) GetInternalTarget(arn string) *listenerLogger {
	en.internal.rwMutex.RLock()
	defer en.internal.rwMutex.RUnlock()
	return en.internal.targets[arn]
}

// Set a new sns target for an input sns ARN.
func (en *eventNotifier) AddListenerChan(snsARN string, listenerCh chan []NotificationEvent) error {
	if listenerCh == nil {
		return errInvalidArgument
	}
	en.internal.rwMutex.Lock()
	defer en.internal.rwMutex.Unlock()
	en.internal.connectedListeners[snsARN] = listenerCh
	return nil
}

// Remove sns target for an input sns ARN.
func (en *eventNotifier) RemoveListenerChan(snsARN string) {
	en.internal.rwMutex.Lock()
	defer en.internal.rwMutex.Unlock()
	if en.internal.connectedListeners != nil {
		delete(en.internal.connectedListeners, snsARN)
	}
}

func (en *eventNotifier) SendListenerEvent(arn string, event []NotificationEvent) error {
	en.internal.rwMutex.Lock()
	defer en.internal.rwMutex.Unlock()

	ch, ok := en.internal.connectedListeners[arn]
	if ok {
		ch <- event
	}
	// If the channel is not present we ignore the event.
	return nil
}

// Fetch bucket notification config for an input bucket.
func (en eventNotifier) GetBucketNotificationConfig(bucket string) *notificationConfig {
	en.external.rwMutex.RLock()
	defer en.external.rwMutex.RUnlock()
	return en.external.notificationConfigs[bucket]
}

func (en *eventNotifier) SetBucketNotificationConfig(bucket string, ncfg *notificationConfig) {
	en.external.rwMutex.Lock()
	if ncfg == nil {
		delete(en.external.notificationConfigs, bucket)
	} else {
		en.external.notificationConfigs[bucket] = ncfg
	}
	en.external.rwMutex.Unlock()
}

func (en *eventNotifier) GetBucketListenerConfig(bucket string) []listenerConfig {
	en.internal.rwMutex.RLock()
	defer en.internal.rwMutex.RUnlock()
	return en.internal.listenerConfigs[bucket]
}

func (en *eventNotifier) SetBucketListenerConfig(bucket string, lcfg []listenerConfig) error {
	en.internal.rwMutex.Lock()
	defer en.internal.rwMutex.Unlock()
	if len(lcfg) == 0 {
		delete(en.internal.listenerConfigs, bucket)
	} else {
		en.internal.listenerConfigs[bucket] = lcfg
	}
	for _, elcArr := range en.internal.listenerConfigs {
		for _, elcElem := range elcArr {
			currArn := elcElem.TopicConfig.TopicARN
			logger, err := newListenerLogger(currArn, elcElem.TargetServer)
			if err != nil {
				return err
			}
			en.internal.targets[currArn] = logger
		}
	}
	return nil
}

func eventNotifyForBucketNotifications(eventType, objectName, bucketName string, nEvent []NotificationEvent) {
	nConfig := globalEventNotifier.GetBucketNotificationConfig(bucketName)
	if nConfig == nil {
		return
	}
	// Validate if the event and object match the queue configs.
	for _, qConfig := range nConfig.QueueConfigs {
		eventMatch := eventMatch(eventType, qConfig.Events)
		ruleMatch := filterRuleMatch(objectName, qConfig.Filter.Key.FilterRules)
		if eventMatch && ruleMatch {
			targetLog := globalEventNotifier.GetExternalTarget(qConfig.QueueARN)
			if targetLog != nil {
				targetLog.WithFields(logrus.Fields{
					"Key":       path.Join(bucketName, objectName),
					"EventType": eventType,
					"Records":   nEvent,
				}).Info()
			}
		}
	}
}

func eventNotifyForBucketListeners(eventType, objectName, bucketName string,
	nEvent []NotificationEvent) {
	lCfgs := globalEventNotifier.GetBucketListenerConfig(bucketName)
	if lCfgs == nil {
		return
	}
	// Validate if the event and object match listener configs
	for _, lcfg := range lCfgs {
		ruleMatch := filterRuleMatch(objectName, lcfg.TopicConfig.Filter.Key.FilterRules)
		eventMatch := eventMatch(eventType, lcfg.TopicConfig.Events)
		if eventMatch && ruleMatch {
			targetLog := globalEventNotifier.GetInternalTarget(
				lcfg.TopicConfig.TopicARN)
			if targetLog != nil && targetLog.log != nil {
				targetLog.log.WithFields(logrus.Fields{
					"Key":       path.Join(bucketName, objectName),
					"EventType": eventType,
					"Records":   nEvent,
				}).Info()
			}
		}
	}

}

// eventNotify notifies an event to relevant targets based on their
// bucket configuration (notifications and listeners).
func eventNotify(event eventData) {
	// Notifies a new event.
	// List of events reported through this function are
	//  - s3:ObjectCreated:Put
	//  - s3:ObjectCreated:Post
	//  - s3:ObjectCreated:Copy
	//  - s3:ObjectCreated:CompleteMultipartUpload
	//  - s3:ObjectRemoved:Delete

	// Event type.
	eventType := event.Type.String()

	// Object name.
	objectName := event.ObjInfo.Name

	// Save the notification event to be sent.
	notificationEvent := []NotificationEvent{newNotificationEvent(event)}

	// Notify external targets.
	eventNotifyForBucketNotifications(eventType, objectName, event.Bucket, notificationEvent)

	// Notify internal targets.
	eventNotifyForBucketListeners(eventType, objectName, event.Bucket, notificationEvent)
}

// loads notification config if any for a given bucket, returns
// structured notification config.
func loadNotificationConfig(bucket string, objAPI ObjectLayer) (*notificationConfig, error) {
	// Construct the notification config path.
	notificationConfigPath := path.Join(bucketConfigPrefix, bucket, bucketNotificationConfig)
	objInfo, err := objAPI.GetObjectInfo(minioMetaBucket, notificationConfigPath)
	if err != nil {
		// 'notification.xml' not found return
		// 'errNoSuchNotifications'.  This is default when no
		// bucket notifications are found on the bucket.
		if isErrObjectNotFound(err) || isErrIncompleteBody(err) {
			return nil, errNoSuchNotifications
		}
		errorIf(err, "Unable to load bucket-notification for bucket %s", bucket)
		// Returns error for other errors.
		return nil, err
	}
	var buffer bytes.Buffer
	err = objAPI.GetObject(minioMetaBucket, notificationConfigPath, 0, objInfo.Size, &buffer)
	if err != nil {
		// 'notification.xml' not found return
		// 'errNoSuchNotifications'.  This is default when no
		// bucket notifications are found on the bucket.
		if isErrObjectNotFound(err) || isErrIncompleteBody(err) {
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
	}

	// Return success.
	return notificationCfg, nil
}

// loads notification config if any for a given bucket, returns
// structured notification config.
func loadListenerConfig(bucket string, objAPI ObjectLayer) ([]listenerConfig, error) {
	// in single node mode, there are no peers, so in this case
	// there is no configuration to load, as any previously
	// connected listen clients have been disconnected
	if !globalIsDistXL {
		return nil, nil
	}

	// Construct the notification config path.
	listenerConfigPath := path.Join(bucketConfigPrefix, bucket, bucketListenerConfig)
	objInfo, err := objAPI.GetObjectInfo(minioMetaBucket, listenerConfigPath)
	if err != nil {
		// 'listener.json' not found return
		// 'errNoSuchNotifications'.  This is default when no
		// bucket notifications are found on the bucket.
		if isErrObjectNotFound(err) {
			return nil, errNoSuchNotifications
		}
		errorIf(err, "Unable to load bucket-listeners for bucket %s", bucket)
		// Returns error for other errors.
		return nil, err
	}
	var buffer bytes.Buffer
	err = objAPI.GetObject(minioMetaBucket, listenerConfigPath, 0, objInfo.Size, &buffer)
	if err != nil {
		// 'notification.xml' not found return
		// 'errNoSuchNotifications'.  This is default when no
		// bucket listeners are found on the bucket.
		if isErrObjectNotFound(err) {
			return nil, errNoSuchNotifications
		}
		errorIf(err, "Unable to load bucket-listeners for bucket %s", bucket)
		// Returns error for other errors.
		return nil, err
	}

	// Unmarshal notification bytes.
	var lCfg []listenerConfig
	lConfigBytes := buffer.Bytes()
	if err = json.Unmarshal(lConfigBytes, &lCfg); err != nil {
		errorIf(err, "Unable to unmarshal listener config from JSON.")
		return nil, err
	}

	// Return success.
	return lCfg, nil
}

func persistNotificationConfig(bucket string, ncfg *notificationConfig, obj ObjectLayer) error {
	// marshal to xml
	buf, err := xml.Marshal(ncfg)
	if err != nil {
		errorIf(err, "Unable to marshal notification configuration into XML")
		return err
	}

	// build path
	ncPath := path.Join(bucketConfigPrefix, bucket, bucketNotificationConfig)
	// write object to path
	sha256Sum := getSHA256Hash(buf)
	_, err = obj.PutObject(minioMetaBucket, ncPath, int64(len(buf)), bytes.NewReader(buf), nil, sha256Sum)
	if err != nil {
		errorIf(err, "Unable to write bucket notification configuration.")
		return err
	}
	return nil
}

// Persists validated listener config to object layer.
func persistListenerConfig(bucket string, lcfg []listenerConfig, obj ObjectLayer) error {
	buf, err := json.Marshal(lcfg)
	if err != nil {
		errorIf(err, "Unable to marshal listener config to JSON.")
		return err
	}

	// build path
	lcPath := path.Join(bucketConfigPrefix, bucket, bucketListenerConfig)
	// write object to path
	sha256Sum := getSHA256Hash(buf)
	_, err = obj.PutObject(minioMetaBucket, lcPath, int64(len(buf)), bytes.NewReader(buf), nil, sha256Sum)
	if err != nil {
		errorIf(err, "Unable to write bucket listener configuration to object layer.")
	}
	return err
}

// Remove listener configuration from storage layer. Used when a bucket is deleted.
func removeListenerConfig(bucket string, objAPI ObjectLayer) error {
	// make the path
	lcPath := path.Join(bucketConfigPrefix, bucket, bucketListenerConfig)
	// remove it
	return objAPI.DeleteObject(minioMetaBucket, lcPath)
}

// Loads both notification and listener config.
func loadNotificationAndListenerConfig(bucketName string, objAPI ObjectLayer) (nCfg *notificationConfig, lCfg []listenerConfig, err error) {
	// Loads notification config if any.
	nCfg, err = loadNotificationConfig(bucketName, objAPI)
	if err != nil && !isErrIgnored(err, errDiskNotFound, errNoSuchNotifications) {
		return nil, nil, err
	}

	// Loads listener config if any.
	lCfg, err = loadListenerConfig(bucketName, objAPI)
	if err != nil && !isErrIgnored(err, errDiskNotFound, errNoSuchNotifications) {
		return nil, nil, err
	}
	return nCfg, lCfg, nil
}

// loads all bucket notifications if present.
func loadAllBucketNotifications(objAPI ObjectLayer) (map[string]*notificationConfig, map[string][]listenerConfig, error) {
	// List buckets to proceed loading all notification configuration.
	buckets, err := objAPI.ListBuckets()
	if err != nil {
		return nil, nil, err
	}

	nConfigs := make(map[string]*notificationConfig)
	lConfigs := make(map[string][]listenerConfig)

	// Loads all bucket notifications.
	for _, bucket := range buckets {
		// Load persistent notification and listener configurations
		// a given bucket name.
		nConfigs[bucket.Name], lConfigs[bucket.Name], err = loadNotificationAndListenerConfig(bucket.Name, objAPI)
		if err != nil {
			return nil, nil, err
		}
	}

	// Success.
	return nConfigs, lConfigs, nil
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
	// Load all nats targets, initialize their respective loggers.
	for accountID, natsN := range serverConfig.GetNATS() {
		if !natsN.Enable {
			continue
		}
		// Construct the queue ARN for NATS.
		queueARN := minioSqs + serverConfig.GetRegion() + ":" + accountID + ":" + queueTypeNATS
		// Queue target if already initialized we move to the next ARN.
		_, ok := queueTargets[queueARN]
		if ok {
			continue
		}
		// Using accountID we can now initialize a new NATS logrus instance.
		natsLog, err := newNATSNotify(accountID)
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
		queueTargets[queueARN] = natsLog
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
	// Load PostgreSQL targets, initialize their respective loggers.
	for accountID, pgN := range serverConfig.GetPostgreSQL() {
		if !pgN.Enable {
			continue
		}
		// Construct the queue ARN for Postgres.
		queueARN := minioSqs + serverConfig.GetRegion() + ":" + accountID + ":" + queueTypePostgreSQL
		_, ok := queueTargets[queueARN]
		if ok {
			continue
		}
		// Using accountID initialize a new Postgresql logrus instance.
		pgLog, err := newPostgreSQLNotify(accountID)
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
		queueTargets[queueARN] = pgLog
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
	nConfigs, lConfigs, err := loadAllBucketNotifications(objAPI)
	if err != nil {
		errorIf(err, "Error loading bucket notifications - %v", err)
		return err
	}

	// Initializes all queue targets.
	queueTargets, err := loadAllQueueTargets()
	if err != nil {
		return err
	}

	// Initialize internal listener targets
	listenTargets := make(map[string]*listenerLogger)
	for _, listeners := range lConfigs {
		for _, listener := range listeners {
			ln, err := newListenerLogger(
				listener.TopicConfig.TopicARN,
				listener.TargetServer,
			)
			if err != nil {
				errorIf(err, "Unable to initialize listener target logger.")
				//TODO: improve error
				return fmt.Errorf("Error initializing listner target logger - %v", err)
			}
			listenTargets[listener.TopicConfig.TopicARN] = ln
		}
	}

	// Initialize event notifier queue.
	globalEventNotifier = &eventNotifier{
		external: externalNotifier{
			notificationConfigs: nConfigs,
			targets:             queueTargets,
			rwMutex:             &sync.RWMutex{},
		},
		internal: internalNotifier{
			rwMutex:            &sync.RWMutex{},
			targets:            listenTargets,
			listenerConfigs:    lConfigs,
			connectedListeners: make(map[string]chan []NotificationEvent),
		},
	}

	return nil
}
