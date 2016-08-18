package cmd

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
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

func newFlushWriter(writer io.Writer) *flushWriter {
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
		writer *flushWriter
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
