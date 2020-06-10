package kv

import (
	"context"
	"fmt"
	pathutil "path"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestInmemoryBackendWatch(t *testing.T) {
	ctx := context.Background()
	backend := NewInmemoryBackend()

	ctx, cancel := context.WithCancel(ctx)

	eventFired := false
	go backend.Watch(ctx, "base.", func(e *Event) {
		assertEquals(t, e.Key, "base.abc", "Event received but key is not correct")
		assertEquals(t, e.Type, EventTypeCreated, "Event received but type is not correct")
		eventFired = true
	})

	backend.Save(ctx, "base.abc", []byte("abs"))
	time.Sleep(time.Millisecond * 10)
	assertEquals(t, eventFired, true, "Expected event has ot been triggered")

	cancel()
}

func TestInmemoryBackendOps(t *testing.T) {
	ctx := context.Background()
	backend := NewInmemoryBackend()

	assertEquals(t, backend.Info(), "inmemory", "Info()")

	assertError(t, backend.Save(ctx, "a", []byte("a")), "Save")

	result, err := backend.Get(ctx, "a")
	assertError(t, err, "Get")
	assertEquals(t, string(result), "a", "Get")

	err = backend.Delete(ctx, "a")
	assertError(t, err, "Delete")
	val, err := backend.Get(ctx, "a")
	assertEquals(t, err, nil, "Get non existing key must NOT fail")
	if val != nil {
		t.Fatalf("Get non existing key must return nil")
	}

	assertError(t, backend.Save(ctx, "base.abc", []byte("abc")), "Save")
	assertError(t, backend.Save(ctx, "base.ddf", []byte("ddf")), "Save")
	assertError(t, backend.Save(ctx, "base.other", []byte("other")), "Save")

	keys, err := backend.Keys(ctx, "base.")
	assertError(t, err, "Keys()")

	assertEquals(t, len(keys), 3, "Keys result must have three elements")

	assertEquals(t, keys.Contains("abc"), true, fmt.Sprintf("Set: %+v", keys))
	assertEquals(t, keys.Contains("ddf"), true, fmt.Sprintf("Set: %+v", keys))
	assertEquals(t, keys.Contains("other"), true, fmt.Sprintf("Set: %+v", keys))
}

func assertError(t *testing.T, err error, msg string) {
	if err != nil {
		t.Fatalf("%s\n Test %s failed with error: %s", msg, getSource(2), err)
	}
}

func assertEquals(t *testing.T, gotValue interface{}, expectedValue interface{}, msg string) {
	if !reflect.DeepEqual(gotValue, expectedValue) {
		t.Fatalf("%s\n Test %s expected '%v', got '%v'", msg, getSource(2), expectedValue, gotValue)
	}
}

func getSource(n int) string {
	var funcName string
	pc, filename, lineNum, ok := runtime.Caller(n)
	if ok {
		filename = pathutil.Base(filename)
		funcName = strings.TrimPrefix(runtime.FuncForPC(pc).Name(),
			"github.com/minio/minio/cmd.")
	} else {
		filename = "<unknown>"
		lineNum = 0
	}

	return fmt.Sprintf("[%s:%d:%s()]", filename, lineNum, funcName)
}
