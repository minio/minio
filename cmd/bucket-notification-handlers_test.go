package cmd

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

// Implement a dummy flush writer.
type flushWriter struct {
	io.Writer
}

// Flush writer is a dummy writer compatible with http.Flusher and http.ResponseWriter.
func (f *flushWriter) Flush()                            {}
func (f *flushWriter) Write(b []byte) (n int, err error) { return f.Writer.Write(b) }
func (f *flushWriter) Header() http.Header               { return http.Header{} }
func (f *flushWriter) WriteHeader(code int)              {}

func newFlushWriter(writer io.Writer) http.ResponseWriter {
	return &flushWriter{writer}
}

// Tests write notification code.
func TestWriteNotification(t *testing.T) {
	// Initialize a new test config.
	root, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("Unable to initialize test config %s", err)
	}
	defer removeAll(root)

	var buffer bytes.Buffer
	// Collection of test cases for each event writer.
	testCases := []struct {
		writer http.ResponseWriter
		event  map[string][]NotificationEvent
		err    error
	}{
		// Invalid input argument with writer `nil` - Test - 1
		{
			writer: nil,
			event:  nil,
			err:    errInvalidArgument,
		},
		// Invalid input argument with event `nil` - Test - 2
		{
			writer: newFlushWriter(ioutil.Discard),
			event:  nil,
			err:    errInvalidArgument,
		},
		// Unmarshal and write, validate last 5 bytes. - Test - 3
		{
			writer: newFlushWriter(&buffer),
			event: map[string][]NotificationEvent{
				"Records": {newNotificationEvent(eventData{
					Type:   ObjectCreatedPut,
					Bucket: "testbucket",
					ObjInfo: ObjectInfo{
						Name: "key",
					},
					ReqParams: map[string]string{
						"ip": "10.1.10.1",
					}}),
				},
			},
			err: nil,
		},
	}
	// Validates all the testcases for writing notification.
	for _, testCase := range testCases {
		err := writeNotification(testCase.writer, testCase.event)
		if err != testCase.err {
			t.Errorf("Unable to write notification %s", err)
		}
		// Validates if the ending string has 'crlf'
		if err == nil && !bytes.HasSuffix(buffer.Bytes(), crlf) {
			buf := buffer.Bytes()[buffer.Len()-5 : 0]
			t.Errorf("Invalid suffix found from the writer last 5 bytes %s, expected `\r\n`", string(buf))
		}
		// Not printing 'buf' on purpose, validates look for string '10.1.10.1'.
		if err == nil && !bytes.Contains(buffer.Bytes(), []byte("10.1.10.1")) {
			// Enable when debugging)
			// fmt.Println(string(buffer.Bytes()))
			t.Errorf("Requested content couldn't be found, expected `10.1.10.1`")
		}
	}
}

func TestSendBucketNotification(t *testing.T) {
	// Initialize a new test config.
	root, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("Unable to initialize test config %s", err)
	}
	defer removeAll(root)

	eventCh := make(chan []NotificationEvent)

	// Create a Pipe with FlushWriter on the write-side and bufio.Scanner
	// on the reader-side to receive notification over the listen channel in a
	// synchronized manner.
	pr, pw := io.Pipe()
	fw := newFlushWriter(pw)
	scanner := bufio.NewScanner(pr)
	// Start a go-routine to wait for notification events.
	go func(listenerCh <-chan []NotificationEvent) {
		sendBucketNotification(fw, listenerCh)
	}(eventCh)

	// Construct notification events to be passed on the events channel.
	var events []NotificationEvent
	evTypes := []EventName{
		ObjectCreatedPut,
		ObjectCreatedPost,
		ObjectCreatedCopy,
		ObjectCreatedCompleteMultipartUpload,
	}
	for _, evType := range evTypes {
		events = append(events, newNotificationEvent(eventData{
			Type: evType,
		}))
	}
	// Send notification events to the channel on which sendBucketNotification
	// is waiting on.
	eventCh <- events

	// Read from the pipe connected to the ResponseWriter.
	scanner.Scan()
	notificationBytes := scanner.Bytes()

	// Close the read-end and send an empty notification event on the channel
	// to signal sendBucketNotification to terminate.
	pr.Close()
	eventCh <- []NotificationEvent{}
	close(eventCh)

	// Checking if the notification are the same as those sent over the channel.
	var notifications map[string][]NotificationEvent
	err = json.Unmarshal(notificationBytes, &notifications)
	if err != nil {
		t.Fatal("Failed to Unmarshal notification")
	}
	records := notifications["Records"]
	for i, rec := range records {
		if rec.EventName == evTypes[i].String() {
			continue
		}
		t.Errorf("Failed to receive %d event %s", i, evTypes[i].String())
	}
}

func initMockEventNotifier(objAPI ObjectLayer) error {
	if objAPI == nil {
		return errInvalidArgument
	}
	globalEventNotifier = &eventNotifier{
		rwMutex:             &sync.RWMutex{},
		queueTargets:        nil,
		notificationConfigs: make(map[string]*notificationConfig),
		snsTargets:          make(map[string][]chan []NotificationEvent),
	}
	return nil
}

func testGetBucketNotificationHandler(obj ObjectLayer, instanceType string, t TestErrHandler) {
	// get random bucket name.
	bucketName := getRandomBucketName()

	// Create bucket to add notification config for.
	err := obj.MakeBucket(bucketName)
	if err != nil {
		// failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err)
	}

	noNotificationBucket := "nonotification"
	err = obj.MakeBucket(noNotificationBucket)
	if err != nil {
		// failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err)
	}

	// Register the API end points with XL/FS object layer.
	apiRouter := initTestAPIEndPoints(obj, []string{
		"GetBucketNotificationHandler",
		"PutBucketNotificationHandler",
	})

	// initialize the server and obtain the credentials and root.
	// credentials are necessary to sign the HTTP request.
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("Init Test config failed")
	}
	// remove the root folder after the test ends.
	defer removeAll(rootPath)

	credentials := serverConfig.GetCredential()

	//Initialize global event notifier with mock queue targets.
	err = initMockEventNotifier(obj)
	if err != nil {
		t.Fatalf("Test %s: Failed to initialize mock event notifier %v",
			instanceType, err)
	}
	// Initialize httptest recorder.
	rec := httptest.NewRecorder()

	// Initialize sample bucket notification config.
	sampleNotificationConfig := []byte("<NotificationConfiguration><TopicConfiguration>" +
		"<Event>s3:ObjectCreated:*</Event><Event>s3:ObjectRemoved:*</Event><Filter>" +
		"<S3Key></S3Key></Filter><Id></Id><Topic>arn:minio:sns:us-east-1:1474332374:listen</Topic>" +
		"</TopicConfiguration></NotificationConfiguration>")

	// Prepare notification config for one of the test cases.
	req, err := newTestSignedRequest("PUT", getPutBucketNotificationURL("", bucketName),
		int64(len(sampleNotificationConfig)), bytes.NewReader(sampleNotificationConfig),
		credentials.AccessKeyID, credentials.SecretAccessKey)
	if err != nil {
		t.Fatalf("Test %d %s: Failed to create HTTP request for PutBucketNotification: <ERROR> %v",
			1, instanceType, err)
	}

	apiRouter.ServeHTTP(rec, req)
	// Test 1: Check if we get back sample notification.
	req, err = newTestSignedRequest("GET", getGetBucketNotificationURL("", bucketName),
		int64(0), nil, credentials.AccessKeyID, credentials.SecretAccessKey)
	if err != nil {
		t.Fatalf("Test %d: %s: Failed to create HTTP request for GetBucketNotification: <ERROR> %v",
			1, instanceType, err)
	}

	apiRouter.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("Test %d: %s: GetBucketNotification request failed with %d: <ERROR> %v",
			2, instanceType, rec.Code, err)
	}
	rspBytes, err := ioutil.ReadAll(rec.Body)
	if err != nil {
		t.Errorf("Test %d: %s: Failed to read response body: <ERROR> %v", 1, instanceType, err)
	}
	if !bytes.Equal(rspBytes, sampleNotificationConfig) {
		t.Errorf("Test %d: %s: Notification config doesn't match expected value: <ERROR> %v", 2, instanceType, err)
	}

	// Test 2: Try getting bucket notification on a non-existent bucket.
	invalidBucketName := "Invalid_BucketName"
	req, err = newTestSignedRequest("GET", getGetBucketNotificationURL("", invalidBucketName),
		int64(0), nil, credentials.AccessKeyID, credentials.SecretAccessKey)
	if err != nil {
		t.Fatalf("Test %d: %s: Failed to create HTTP request for GetBucketNotification: <ERROR> %v",
			2, instanceType, err)
	}
	apiRouter.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("Test %d: %s: GetBucketNotification request failed with %d: <ERROR> %v",
			2, instanceType, rec.Code, err)
	}

	// Test 3: Try getting bucket notification for a bucket with notification set.
	emptyNotificationXML := []byte("<NotificationConfiguration></NotificationConfiguration>" +
		"<NotificationConfiguration></NotificationConfiguration>")
	req, err = newTestSignedRequest("GET", getGetBucketNotificationURL("", noNotificationBucket),
		int64(0), nil, credentials.AccessKeyID, credentials.SecretAccessKey)
	if err != nil {
		t.Fatalf("Test %d: %s: Failed to create HTTP request for GetBucketNotification: <ERROR> %v",
			3, instanceType, err)
	}
	apiRouter.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("Test %d: %s: GetBucketNotification request failed with %d: <ERROR> %v",
			3, instanceType, rec.Code, err)
	}
	if !bytes.Equal(rec.Body.Bytes(), emptyNotificationXML) {
		t.Errorf("Test %d: %s: GetBucketNotification request received notification "+
			"config different from empty config: <ERROR> %v", 3, instanceType, err)
	}
}

func TestGetBucketNotificationHandler(t *testing.T) {
	ExecObjectLayerTest(t, testGetBucketNotificationHandler)
}
