package cmd

import (
	"strconv"
	"testing"
	"time"
)

func timeInFuture() string {
	return strconv.FormatInt(time.Now().Unix()+10*60, 10)
}

func timeInPast() string {
	return strconv.FormatInt(time.Now().Unix()-10*60, 10)
}

func TestFilterExpiredItems(t *testing.T) {
	t.Run("no items", func(t *testing.T) {
		items := []iamConfigItem{}
		filteredItems := filterExpiredItems(items)
		if len(filteredItems) != 0 {
			t.Errorf("Expected empty. Was not")
		}
	})

	t.Run("no expired items", func(t *testing.T) {
		items := []iamConfigItem{
			{objPath: "some-path", data: "jsonData"},
			{objPath: addTTLPrefix("some-path"), data: timeInFuture()},
			{objPath: "some-other-path", data: "jsonData"},
			{objPath: addTTLPrefix("some-other-path"), data: timeInFuture()},
		}
		filteredItems := filterExpiredItems(items)
		if len(filteredItems) != 4 {
			t.Errorf("Expected no items to be filtered")
		}
	})

	t.Run("all expired items", func(t *testing.T) {
		items := []iamConfigItem{
			{objPath: "some-path", data: "jsonData"},
			{objPath: addTTLPrefix("some-path"), data: timeInPast()},
			{objPath: "some-other-path", data: "jsonData"},
			{objPath: addTTLPrefix("some-other-path"), data: timeInPast()},
		}
		filteredItems := filterExpiredItems(items)
		if len(filteredItems) != 0 {
			t.Errorf("Expected all items to be filtered")
		}
	})

	t.Run("some expired items", func(t *testing.T) {
		items := []iamConfigItem{
			{objPath: "some-path", data: "jsonData"},
			{objPath: addTTLPrefix("some-path"), data: timeInFuture()},
			{objPath: "some-other-path", data: "jsonData"},
			{objPath: addTTLPrefix("some-other-path"), data: timeInPast()},
		}
		filteredItems := filterExpiredItems(items)
		if len(filteredItems) != 2 {
			t.Errorf("Expected 2 items to be filtered")
		}
	})
}
