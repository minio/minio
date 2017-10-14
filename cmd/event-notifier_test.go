/*
 * Minio Cloud Storage, (C) 2016, 2017 Minio, Inc.
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
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"
)

// Test InitEventNotifier with faulty disks
func TestInitEventNotifierFaultyDisks(t *testing.T) {
	// Prepare for tests
	rootPath, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatalf("Init Test config failed")
	}
	// remove the root directory after the test ends.
	defer os.RemoveAll(rootPath)

	disks, err := getRandomDisks(4)
	if err != nil {
		t.Fatal("Unable to create directories for FS backend. ", err)
	}
	defer removeRoots(disks)
	obj, _, err := initObjectLayer(mustGetNewEndpointList(disks...))
	if err != nil {
		t.Fatal("Unable to initialize FS backend.", err)
	}

	bucketName := "bucket"
	if err := obj.MakeBucketWithLocation(bucketName, ""); err != nil {
		t.Fatal("Unexpected error:", err)
	}

	xl := obj.(*xlObjects)

	listenARN := "arn:minio:sns:us-east-1:1:listen"
	queueARN := "arn:minio:sqs:us-east-1:1:redis"

	// Write a notification.xml in the disk
	notificationXML := "<NotificationConfiguration>"
	notificationXML += "<TopicConfiguration><Event>s3:ObjectRemoved:*</Event><Event>s3:ObjectRemoved:*</Event><Topic>" + listenARN + "</Topic></TopicConfiguration>"
	notificationXML += "<QueueConfiguration><Event>s3:ObjectRemoved:*</Event><Event>s3:ObjectRemoved:*</Event><Queue>" + queueARN + "</Queue></QueueConfiguration>"
	notificationXML += "</NotificationConfiguration>"
	size := int64(len([]byte(notificationXML)))
	reader := bytes.NewReader([]byte(notificationXML))
	if _, err := xl.PutObject(minioMetaBucket, bucketConfigPrefix+"/"+bucketName+"/"+bucketNotificationConfig, mustGetHashReader(t, reader, size, "", ""), nil); err != nil {
		t.Fatal("Unexpected error:", err)
	}

	for i, d := range xl.storageDisks {
		xl.storageDisks[i] = newNaughtyDisk(d.(*retryStorage), nil, errFaultyDisk)
	}
	// Test initEventNotifier() with faulty disks
	for i := 1; i <= 3; i++ {
		if err := initEventNotifier(xl); errorCause(err) != errFaultyDisk {
			t.Fatal("Unexpected error:", err)
		}
	}
}

// InitEventNotifierWithPostgreSQL - tests InitEventNotifier when PostgreSQL is not prepared
func TestInitEventNotifierWithPostgreSQL(t *testing.T) {
	// initialize the server and obtain the credentials and root.
	// credentials are necessary to sign the HTTP request.
	rootPath, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatalf("Init Test config failed")
	}
	// remove the root directory after the test ends.
	defer os.RemoveAll(rootPath)

	disks, err := getRandomDisks(1)
	defer os.RemoveAll(disks[0])
	if err != nil {
		t.Fatal("Unable to create directories for FS backend. ", err)
	}
	fs, _, err := initObjectLayer(mustGetNewEndpointList(disks...))
	if err != nil {
		t.Fatal("Unable to initialize FS backend.", err)
	}

	serverConfig.Notify.SetPostgreSQLByID("1", postgreSQLNotify{Enable: true})
	if err := initEventNotifier(fs); err == nil {
		t.Fatal("PostgreSQL config didn't fail.")
	}
}

// InitEventNotifierWithNATS - tests InitEventNotifier when NATS is not prepared
func TestInitEventNotifierWithNATS(t *testing.T) {
	// initialize the server and obtain the credentials and root.
	// credentials are necessary to sign the HTTP request.
	rootPath, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatalf("Init Test config failed")
	}
	// remove the root directory after the test ends.
	defer os.RemoveAll(rootPath)

	disks, err := getRandomDisks(1)
	defer os.RemoveAll(disks[0])
	if err != nil {
		t.Fatal("Unable to create directories for FS backend. ", err)
	}
	fs, _, err := initObjectLayer(mustGetNewEndpointList(disks...))
	if err != nil {
		t.Fatal("Unable to initialize FS backend.", err)
	}

	serverConfig.Notify.SetNATSByID("1", natsNotify{Enable: true})
	if err := initEventNotifier(fs); err == nil {
		t.Fatal("NATS config didn't fail.")
	}
}

// InitEventNotifierWithWebHook - tests InitEventNotifier when WebHook is not prepared
func TestInitEventNotifierWithWebHook(t *testing.T) {
	// initialize the server and obtain the credentials and root.
	// credentials are necessary to sign the HTTP request.
	rootPath, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatalf("Init Test config failed")
	}
	// remove the root directory after the test ends.
	defer os.RemoveAll(rootPath)

	disks, err := getRandomDisks(1)
	defer os.RemoveAll(disks[0])
	if err != nil {
		t.Fatal("Unable to create directories for FS backend. ", err)
	}
	fs, _, err := initObjectLayer(mustGetNewEndpointList(disks...))
	if err != nil {
		t.Fatal("Unable to initialize FS backend.", err)
	}

	serverConfig.Notify.SetWebhookByID("1", webhookNotify{Enable: true})
	if err := initEventNotifier(fs); err == nil {
		t.Fatal("WebHook config didn't fail.")
	}
}

// InitEventNotifierWithAMQP - tests InitEventNotifier when AMQP is not prepared
func TestInitEventNotifierWithAMQP(t *testing.T) {
	// initialize the server and obtain the credentials and root.
	// credentials are necessary to sign the HTTP request.
	rootPath, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatalf("Init Test config failed")
	}
	// remove the root directory after the test ends.
	defer os.RemoveAll(rootPath)

	disks, err := getRandomDisks(1)
	defer os.RemoveAll(disks[0])
	if err != nil {
		t.Fatal("Unable to create directories for FS backend. ", err)
	}
	fs, _, err := initObjectLayer(mustGetNewEndpointList(disks...))
	if err != nil {
		t.Fatal("Unable to initialize FS backend.", err)
	}

	serverConfig.Notify.SetAMQPByID("1", amqpNotify{Enable: true})
	if err := initEventNotifier(fs); err == nil {
		t.Fatal("AMQP config didn't fail.")
	}
}

// InitEventNotifierWithElasticSearch - test InitEventNotifier when ElasticSearch is not ready
func TestInitEventNotifierWithElasticSearch(t *testing.T) {
	// initialize the server and obtain the credentials and root.
	// credentials are necessary to sign the HTTP request.
	rootPath, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatalf("Init Test config failed")
	}
	// remove the root directory after the test ends.
	defer os.RemoveAll(rootPath)

	disks, err := getRandomDisks(1)
	defer os.RemoveAll(disks[0])
	if err != nil {
		t.Fatal("Unable to create directories for FS backend. ", err)
	}
	fs, _, err := initObjectLayer(mustGetNewEndpointList(disks...))
	if err != nil {
		t.Fatal("Unable to initialize FS backend.", err)
	}

	serverConfig.Notify.SetElasticSearchByID("1", elasticSearchNotify{Enable: true})
	if err := initEventNotifier(fs); err == nil {
		t.Fatal("ElasticSearch config didn't fail.")
	}
}

// InitEventNotifierWithRedis - test InitEventNotifier when Redis is not ready
func TestInitEventNotifierWithRedis(t *testing.T) {
	// initialize the server and obtain the credentials and root.
	// credentials are necessary to sign the HTTP request.
	rootPath, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatalf("Init Test config failed")
	}
	// remove the root directory after the test ends.
	defer os.RemoveAll(rootPath)

	disks, err := getRandomDisks(1)
	defer os.RemoveAll(disks[0])
	if err != nil {
		t.Fatal("Unable to create directories for FS backend. ", err)
	}
	fs, _, err := initObjectLayer(mustGetNewEndpointList(disks...))
	if err != nil {
		t.Fatal("Unable to initialize FS backend.", err)
	}

	serverConfig.Notify.SetRedisByID("1", redisNotify{Enable: true})
	if err := initEventNotifier(fs); err == nil {
		t.Fatal("Redis config didn't fail.")
	}
}

type TestPeerRPCServerData struct {
	serverType string
	testServer TestServer
}

func (s *TestPeerRPCServerData) Setup(t *testing.T) {
	s.testServer = StartTestPeersRPCServer(t, s.serverType)

	// setup port and minio addr
	host, port := mustSplitHostPort(s.testServer.Server.Listener.Addr().String())
	globalMinioHost = host
	globalMinioPort = port
	globalMinioAddr = getEndpointsLocalAddr(s.testServer.endpoints)

	// initialize the peer client(s)
	initGlobalS3Peers(s.testServer.Disks)
}

func (s *TestPeerRPCServerData) TearDown() {
	s.testServer.Stop()
	_ = os.RemoveAll(s.testServer.Root)
	for _, d := range s.testServer.Disks {
		_ = os.RemoveAll(d.Path)
	}
}

func TestSetNGetBucketNotification(t *testing.T) {
	s := TestPeerRPCServerData{serverType: "XL"}

	// setup and teardown
	s.Setup(t)
	defer s.TearDown()

	bucketName := getRandomBucketName()

	obj := s.testServer.Obj
	if err := initEventNotifier(obj); err != nil {
		t.Fatal("Unexpected error:", err)
	}

	globalEventNotifier.SetBucketNotificationConfig(bucketName, &notificationConfig{})
	nConfig := globalEventNotifier.GetBucketNotificationConfig(bucketName)
	if nConfig == nil {
		t.Errorf("Notification expected to be set, but notification not set.")
	}

	if !reflect.DeepEqual(nConfig, &notificationConfig{}) {
		t.Errorf("Mismatching notification configs.")
	}
}

func TestInitEventNotifier(t *testing.T) {
	currentIsDistXL := globalIsDistXL
	defer func() {
		globalIsDistXL = currentIsDistXL
	}()

	s := TestPeerRPCServerData{serverType: "XL"}

	// setup and teardown
	s.Setup(t)
	defer s.TearDown()

	// test if empty object layer arg. returns expected error.
	if err := initEventNotifier(nil); err == nil || err != errInvalidArgument {
		t.Fatalf("initEventNotifier returned unexpected error value - %v", err)
	}

	obj := s.testServer.Obj
	bucketName := getRandomBucketName()
	// declare sample configs
	filterRules := []filterRule{
		{
			Name:  "prefix",
			Value: "minio",
		},
		{
			Name:  "suffix",
			Value: "*.jpg",
		},
	}
	sampleSvcCfg := ServiceConfig{
		[]string{"s3:ObjectRemoved:*", "s3:ObjectCreated:*"},
		filterStruct{
			keyFilter{filterRules},
		},
		"1",
	}
	sampleNotifCfg := notificationConfig{
		QueueConfigs: []queueConfig{
			{
				ServiceConfig: sampleSvcCfg,
				QueueARN:      "testqARN",
			},
		},
	}
	sampleListenCfg := []listenerConfig{
		{
			TopicConfig: topicConfig{ServiceConfig: sampleSvcCfg,
				TopicARN: "testlARN"},
			TargetServer: globalMinioAddr,
		},
	}

	// create bucket
	if err := obj.MakeBucketWithLocation(bucketName, ""); err != nil {
		t.Fatal("Unexpected error:", err)
	}

	// bucket is created, now writing should not give errors.
	if err := persistNotificationConfig(bucketName, &sampleNotifCfg, obj); err != nil {
		t.Fatal("Unexpected error:", err)
	}

	if err := persistListenerConfig(bucketName, sampleListenCfg, obj); err != nil {
		t.Fatal("Unexpected error:", err)
	}

	// needed to load listener config from disk for testing (in
	// single peer mode, the listener config is ignored, but here
	// we want to test the loading from disk too.)
	globalIsDistXL = true

	// test event notifier init
	if err := initEventNotifier(obj); err != nil {
		t.Fatal("Unexpected error:", err)
	}

	// fetch bucket configs and verify
	ncfg := globalEventNotifier.GetBucketNotificationConfig(bucketName)
	if ncfg == nil {
		t.Error("Bucket notification was not present for ", bucketName)
	}
	if len(ncfg.QueueConfigs) != 1 || ncfg.QueueConfigs[0].QueueARN != "testqARN" {
		t.Error("Unexpected bucket notification found - ", *ncfg)
	}
	if globalEventNotifier.GetExternalTarget("testqARN") != nil {
		t.Error("A logger was not expected to be found as it was not enabled in the config.")
	}

	lcfg := globalEventNotifier.GetBucketListenerConfig(bucketName)
	if lcfg == nil {
		t.Error("Bucket listener was not present for ", bucketName)
	}
	if len(lcfg) != 1 || lcfg[0].TargetServer != globalMinioAddr || lcfg[0].TopicConfig.TopicARN != "testlARN" {
		t.Error("Unexpected listener config found - ", lcfg[0])
	}
	if globalEventNotifier.GetInternalTarget("testlARN") == nil {
		t.Error("A listen logger was not found.")
	}
}

func TestListenBucketNotification(t *testing.T) {
	currentIsDistXL := globalIsDistXL
	defer func() {
		globalIsDistXL = currentIsDistXL
	}()

	s := TestPeerRPCServerData{serverType: "XL"}
	// setup and teardown
	s.Setup(t)
	defer s.TearDown()

	// test initialisation
	obj := s.testServer.Obj

	bucketName := "bucket"
	objectName := "object"

	// Create the bucket to listen on
	if err := obj.MakeBucketWithLocation(bucketName, ""); err != nil {
		t.Fatal("Unexpected error:", err)
	}

	listenARN := fmt.Sprintf("%s:%s:1:%s-%s",
		minioTopic,
		serverConfig.GetRegion(),
		snsTypeMinio,
		s.testServer.Server.Listener.Addr(),
	)
	lcfg := listenerConfig{
		TopicConfig: topicConfig{
			ServiceConfig{
				[]string{"s3:ObjectRemoved:*", "s3:ObjectCreated:*"},
				filterStruct{},
				"0",
			},
			listenARN,
		},
		TargetServer: globalMinioAddr,
	}

	// write listener config to storage layer
	lcfgs := []listenerConfig{lcfg}
	if err := persistListenerConfig(bucketName, lcfgs, obj); err != nil {
		t.Fatalf("Test Setup error: %v", err)
	}

	// needed to load listener config from disk for testing (in
	// single peer mode, the listener config is ingored, but here
	// we want to test the loading from disk too.)
	globalIsDistXL = true

	// Init event notifier
	if err := initEventNotifier(obj); err != nil {
		t.Fatal("Unexpected error:", err)
	}

	// Check if the config is loaded
	listenerCfg := globalEventNotifier.GetBucketListenerConfig(bucketName)
	if listenerCfg == nil {
		t.Fatal("Cannot load bucket listener config")
	}
	if len(listenerCfg) != 1 {
		t.Fatal("Listener config is not correctly loaded. Exactly one listener config is expected")
	}

	// Check if topic ARN is correct
	if listenerCfg[0].TopicConfig.TopicARN != listenARN {
		t.Fatal("Configured topic ARN is incorrect.")
	}

	// Create a new notification event channel.
	nEventCh := make(chan []NotificationEvent)
	// Close the listener channel.
	defer close(nEventCh)
	// Add events channel for listener.
	if err := globalEventNotifier.AddListenerChan(listenARN, nEventCh); err != nil {
		t.Fatalf("Test Setup error: %v", err)
	}
	// Remove listen channel after the writer has closed or the
	// client disconnected.
	defer globalEventNotifier.RemoveListenerChan(listenARN)

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
			t.Fatal("Unexpected error occurred")
		}
		if n[0].S3.Object.Key != objectName {
			t.Fatalf("Received wrong object name in notification, expected %s, received %s", n[0].S3.Object.Key, objectName)
		}
		break
	case <-time.After(3 * time.Second):
		break
	}

}

func TestAddRemoveBucketListenerConfig(t *testing.T) {
	s := TestPeerRPCServerData{serverType: "XL"}

	// setup and teardown
	s.Setup(t)
	defer s.TearDown()

	// test code
	obj := s.testServer.Obj
	if err := initEventNotifier(obj); err != nil {
		t.Fatalf("Failed to initialize event notifier: %v", err)
	}

	// Make a bucket to store topicConfigs.
	randBucket := getRandomBucketName()
	if err := obj.MakeBucketWithLocation(randBucket, ""); err != nil {
		t.Fatalf("Failed to make bucket %s", randBucket)
	}

	// Add a topicConfig to an empty notificationConfig.
	accountID := fmt.Sprintf("%d", UTCNow().UnixNano())
	accountARN := fmt.Sprintf(
		"arn:minio:sqs:%s:%s:listen-%s",
		serverConfig.GetRegion(),
		accountID,
		globalMinioAddr,
	)

	// Make topic configuration
	filterRules := []filterRule{
		{
			Name:  "prefix",
			Value: "minio",
		},
		{
			Name:  "suffix",
			Value: "*.jpg",
		},
	}
	sampleTopicCfg := topicConfig{
		TopicARN: accountARN,
		ServiceConfig: ServiceConfig{
			[]string{"s3:ObjectRemoved:*", "s3:ObjectCreated:*"},
			filterStruct{
				keyFilter{filterRules},
			},
			"sns-" + accountID,
		},
	}
	sampleListenerCfg := &listenerConfig{
		TopicConfig:  sampleTopicCfg,
		TargetServer: globalMinioAddr,
	}
	testCases := []struct {
		lCfg        *listenerConfig
		expectedErr error
	}{
		{sampleListenerCfg, nil},
		{nil, errInvalidArgument},
	}

	for i, test := range testCases {
		err := AddBucketListenerConfig(randBucket, test.lCfg, obj)
		if err != test.expectedErr {
			t.Errorf(
				"Test %d: Failed with error %v, expected to fail with %v",
				i+1, err, test.expectedErr,
			)
		}
	}

	// test remove listener actually removes a listener
	RemoveBucketListenerConfig(randBucket, sampleListenerCfg, obj)
	// since it does not return errors we fetch the config and
	// check
	lcSlice := globalEventNotifier.GetBucketListenerConfig(randBucket)
	if len(lcSlice) != 0 {
		t.Errorf("Remove Listener Config Test: did not remove listener config - %v",
			lcSlice)
	}
}
