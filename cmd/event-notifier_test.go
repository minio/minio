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
	"reflect"
	"testing"
	"time"
)

// Tests event notify.
func TestEventNotify(t *testing.T) {
	ExecObjectLayerTest(t, testEventNotify)
}

func testEventNotify(obj ObjectLayer, instanceType string, t TestErrHandler) {
	bucketName := getRandomBucketName()

	// initialize the server and obtain the credentials and root.
	// credentials are necessary to sign the HTTP request.
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("Init Test config failed")
	}
	// remove the root folder after the test ends.
	defer removeAll(rootPath)

	if err := initEventNotifier(obj); err != nil {
		t.Fatal("Unexpected error:", err)
	}

	// Notify object created event.
	eventNotify(eventData{
		Type:   ObjectCreatedPost,
		Bucket: bucketName,
		ObjInfo: ObjectInfo{
			Bucket: bucketName,
			Name:   "object1",
		},
		ReqParams: map[string]string{
			"sourceIPAddress": "localhost:1337",
		},
	})

	if err := globalEventNotifier.SetBucketNotificationConfig(bucketName, nil); err != errInvalidArgument {
		t.Errorf("Expected error %s, got %s", errInvalidArgument, err)
	}

	if err := globalEventNotifier.SetBucketNotificationConfig(bucketName, &notificationConfig{}); err != nil {
		t.Errorf("Expected error to be nil, got %s", err)
	}

	if !globalEventNotifier.IsBucketNotificationSet(bucketName) {
		t.Errorf("Notification expected to be set, but notification not set.")
	}

	nConfig := globalEventNotifier.GetBucketNotificationConfig(bucketName)
	if !reflect.DeepEqual(nConfig, &notificationConfig{}) {
		t.Errorf("Mismatching notification configs.")
	}

	// Notify object created event.
	eventNotify(eventData{
		Type:   ObjectRemovedDelete,
		Bucket: bucketName,
		ObjInfo: ObjectInfo{
			Bucket: bucketName,
			Name:   "object1",
		},
		ReqParams: map[string]string{
			"sourceIPAddress": "localhost:1337",
		},
	})
}

// Tests various forms of inititalization of event notifier.
func TestInitEventNotifier(t *testing.T) {
	disk, err := getRandomDisks(1)
	if err != nil {
		t.Fatal("Unable to create directories for FS backend. ", err)
	}
	fs, err := getSingleNodeObjectLayer(disk[0])
	if err != nil {
		t.Fatal("Unable to initialize FS backend.", err)
	}
	nDisks := 16
	disks, err := getRandomDisks(nDisks)
	if err != nil {
		t.Fatal("Unable to create directories for XL backend. ", err)
	}
	xl, err := getXLObjectLayer(disks, nil)
	if err != nil {
		t.Fatal("Unable to initialize XL backend.", err)
	}

	disks = append(disks, disk...)
	for _, d := range disks {
		defer removeAll(d)
	}

	// Collection of test cases for inititalizing event notifier.
	testCases := []struct {
		objAPI  ObjectLayer
		configs map[string]*notificationConfig
		err     error
	}{
		// Test 1 - invalid arguments.
		{
			objAPI: nil,
			err:    errInvalidArgument,
		},
		// Test 2 - valid FS object layer but no bucket notifications.
		{
			objAPI: fs,
			err:    nil,
		},
		// Test 3 - valid XL object layer but no bucket notifications.
		{
			objAPI: xl,
			err:    nil,
		},
	}

	// Validate if event notifier is properly initialized.
	for i, testCase := range testCases {
		err = initEventNotifier(testCase.objAPI)
		if err != testCase.err {
			t.Errorf("Test %d: Expected %s, but got: %s", i+1, testCase.err, err)
		}
	}
}

// Test InitEventNotifier with faulty disks
func TestInitEventNotifierFaultyDisks(t *testing.T) {
	// Prepare for tests
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("Init Test config failed")
	}
	// remove the root folder after the test ends.
	defer removeAll(rootPath)

	disk, err := getRandomDisks(1)
	defer removeAll(disk[0])
	if err != nil {
		t.Fatal("Unable to create directories for FS backend. ", err)
	}
	obj, err := getSingleNodeObjectLayer(disk[0])
	if err != nil {
		t.Fatal("Unable to initialize FS backend.", err)
	}

	bucketName := "bucket"
	if err := obj.MakeBucket(bucketName); err != nil {
		t.Fatal("Unexpected error:", err)
	}

	fs := obj.(fsObjects)
	fsstorage := fs.storage.(*posix)

	listenARN := "arn:minio:sns:us-east-1:1:listen"
	queueARN := "arn:minio:sqs:us-east-1:1:redis"

	// Write a notification.xml in the disk
	notificationXML := "<NotificationConfiguration>"
	notificationXML += "<TopicConfiguration><Event>s3:ObjectRemoved:*</Event><Event>s3:ObjectRemoved:*</Event><Topic>" + listenARN + "</Topic></TopicConfiguration>"
	notificationXML += "<QueueConfiguration><Event>s3:ObjectRemoved:*</Event><Event>s3:ObjectRemoved:*</Event><Queue>" + queueARN + "</Queue></QueueConfiguration>"
	notificationXML += "</NotificationConfiguration>"
	if err := fsstorage.AppendFile(minioMetaBucket, bucketConfigPrefix+"/"+bucketName+"/"+bucketNotificationConfig, []byte(notificationXML)); err != nil {
		t.Fatal("Unexpected error:", err)
	}

	// Test initEventNotifier() with faulty disks
	for i := 1; i <= 5; i++ {
		fs.storage = newNaughtyDisk(fsstorage, map[int]error{i: errFaultyDisk}, nil)
		if err := initEventNotifier(fs); errorCause(err) != errFaultyDisk {
			t.Fatal("Unexpected error:", err)
		}
	}
}

// InitEventNotifierWithAMQP - tests InitEventNotifier when AMQP is not prepared
func TestInitEventNotifierWithAMQP(t *testing.T) {
	// initialize the server and obtain the credentials and root.
	// credentials are necessary to sign the HTTP request.
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("Init Test config failed")
	}
	// remove the root folder after the test ends.
	defer removeAll(rootPath)

	disk, err := getRandomDisks(1)
	defer removeAll(disk[0])
	if err != nil {
		t.Fatal("Unable to create directories for FS backend. ", err)
	}
	fs, err := getSingleNodeObjectLayer(disk[0])
	if err != nil {
		t.Fatal("Unable to initialize FS backend.", err)
	}

	serverConfig.SetAMQPNotifyByID("1", amqpNotify{Enable: true})
	if err := initEventNotifier(fs); err == nil {
		t.Fatal("AMQP config didn't fail.")
	}
}

// InitEventNotifierWithElasticSearch - test InitEventNotifier when ElasticSearch is not ready
func TestInitEventNotifierWithElasticSearch(t *testing.T) {
	// initialize the server and obtain the credentials and root.
	// credentials are necessary to sign the HTTP request.
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("Init Test config failed")
	}
	// remove the root folder after the test ends.
	defer removeAll(rootPath)

	disk, err := getRandomDisks(1)
	defer removeAll(disk[0])
	if err != nil {
		t.Fatal("Unable to create directories for FS backend. ", err)
	}
	fs, err := getSingleNodeObjectLayer(disk[0])
	if err != nil {
		t.Fatal("Unable to initialize FS backend.", err)
	}

	serverConfig.SetElasticSearchNotifyByID("1", elasticSearchNotify{Enable: true})
	if err := initEventNotifier(fs); err == nil {
		t.Fatal("ElasticSearch config didn't fail.")
	}
}

// InitEventNotifierWithRedis - test InitEventNotifier when Redis is not ready
func TestInitEventNotifierWithRedis(t *testing.T) {
	// initialize the server and obtain the credentials and root.
	// credentials are necessary to sign the HTTP request.
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("Init Test config failed")
	}
	// remove the root folder after the test ends.
	defer removeAll(rootPath)

	disk, err := getRandomDisks(1)
	defer removeAll(disk[0])
	if err != nil {
		t.Fatal("Unable to create directories for FS backend. ", err)
	}
	fs, err := getSingleNodeObjectLayer(disk[0])
	if err != nil {
		t.Fatal("Unable to initialize FS backend.", err)
	}

	serverConfig.SetRedisNotifyByID("1", redisNotify{Enable: true})
	if err := initEventNotifier(fs); err == nil {
		t.Fatal("Redis config didn't fail.")
	}
}

// TestListenBucketNotification - test Listen Bucket Notification process
func TestListenBucketNotification(t *testing.T) {

	bucketName := "bucket"
	objectName := "object"

	// Prepare for tests
	// Create fs backend
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("Init Test config failed")
	}
	// remove the root folder after the test ends.
	defer removeAll(rootPath)

	disk, err := getRandomDisks(1)
	defer removeAll(disk[0])
	if err != nil {
		t.Fatal("Unable to create directories for FS backend. ", err)
	}
	obj, err := getSingleNodeObjectLayer(disk[0])
	if err != nil {
		t.Fatal("Unable to initialize FS backend.", err)
	}

	// Create the bucket to listen on
	if err := obj.MakeBucket(bucketName); err != nil {
		t.Fatal("Unexpected error:", err)
	}

	listenARN := "arn:minio:sns:us-east-1:1:listen"
	queueARN := "arn:minio:sqs:us-east-1:1:redis"

	fs := obj.(fsObjects)
	storage := fs.storage.(*posix)

	// Create and store notification.xml with listen and queue notification configured
	notificationXML := "<NotificationConfiguration>"
	notificationXML += "<TopicConfiguration><Event>s3:ObjectRemoved:*</Event><Event>s3:ObjectRemoved:*</Event><Topic>" + listenARN + "</Topic></TopicConfiguration>"
	notificationXML += "<QueueConfiguration><Event>s3:ObjectRemoved:*</Event><Event>s3:ObjectRemoved:*</Event><Queue>" + queueARN + "</Queue></QueueConfiguration>"
	notificationXML += "</NotificationConfiguration>"
	if err := storage.AppendFile(minioMetaBucket, bucketConfigPrefix+"/"+bucketName+"/"+bucketNotificationConfig, []byte(notificationXML)); err != nil {
		t.Fatal("Unexpected error:", err)
	}

	// Init event notifier
	if err := initEventNotifier(fs); err != nil {
		t.Fatal("Unexpected error:", err)
	}

	// Validate if minio SNS is configured for an empty topic configs.
	if isMinioSNSConfigured(listenARN, nil) {
		t.Fatal("SNS listen shouldn't be configured.")
	}

	// Check if the config is loaded
	notificationCfg := globalEventNotifier.GetBucketNotificationConfig(bucketName)
	if notificationCfg == nil {
		t.Fatal("Cannot load bucket notification config")
	}
	if len(notificationCfg.TopicConfigs) != 1 || len(notificationCfg.QueueConfigs) != 1 {
		t.Fatal("Notification config is not correctly loaded. Exactly one topic and one queue config are expected")
	}

	// Check if listen notification config is enabled
	if !isMinioSNSConfigured(listenARN, notificationCfg.TopicConfigs) {
		t.Fatal("SNS listen is not configured.")
	}

	// Create a new notification event channel.
	nEventCh := make(chan []NotificationEvent)
	// Close the listener channel.
	defer close(nEventCh)
	// Set sns target.
	globalEventNotifier.SetSNSTarget(listenARN, nEventCh)
	// Remove sns listener after the writer has closed or the client disconnected.
	defer globalEventNotifier.RemoveSNSTarget(listenARN, nEventCh)

	// Fire an event notification
	go eventNotify(eventData{
		Type:   ObjectRemovedDelete,
		Bucket: bucketName,
		ObjInfo: ObjectInfo{
			Bucket: bucketName,
			Name:   objectName,
		},
		ReqParams: map[string]string{
			"sourceIPAddress": "localhost:1337",
		},
	})

	// Wait for the event notification here, if nothing is received within 30 seconds,
	// test error will be fired
	select {
	case n := <-nEventCh:
		// Check that received event
		if len(n) == 0 {
			t.Fatal("Unexpected error occured")
		}
		if n[0].S3.Object.Key != objectName {
			t.Fatalf("Received wrong object name in notification, expected %s, received %s", n[0].S3.Object.Key, objectName)
		}
		break
	case <-time.After(30 * time.Second):
		break
	}
}
