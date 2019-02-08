package strutil

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/hashicorp/errwrap"
	glob "github.com/ryanuber/go-glob"
)

// StrListContainsGlob looks for a string in a list of strings and allows
// globs.
func StrListContainsGlob(haystack []string, needle string) bool {
	for _, item := range haystack {
		if glob.Glob(item, needle) {
			return true
		}
	}
	return false
}

// StrListContains looks for a string in a list of strings.
func StrListContains(haystack []string, needle string) bool {
	for _, item := range haystack {
		if item == needle {
			return true
		}
	}
	return false
}

// StrListSubset checks if a given list is a subset
// of another set
func StrListSubset(super, sub []string) bool {
	for _, item := range sub {
		if !StrListContains(super, item) {
			return false
		}
	}
	return true
}

// ParseDedupAndSortStrings parses a comma separated list of strings
// into a slice of strings. The return slice will be sorted and will
// not contain duplicate or empty items.
func ParseDedupAndSortStrings(input string, sep string) []string {
	input = strings.TrimSpace(input)
	parsed := []string{}
	if input == "" {
		// Don't return nil
		return parsed
	}
	return RemoveDuplicates(strings.Split(input, sep), false)
}

// ParseDedupLowercaseAndSortStrings parses a comma separated list of
// strings into a slice of strings. The return slice will be sorted and
// will not contain duplicate or empty items. The values will be converted
// to lower case.
func ParseDedupLowercaseAndSortStrings(input string, sep string) []string {
	input = strings.TrimSpace(input)
	parsed := []string{}
	if input == "" {
		// Don't return nil
		return parsed
	}
	return RemoveDuplicates(strings.Split(input, sep), true)
}

// ParseKeyValues parses a comma separated list of `<key>=<value>` tuples
// into a map[string]string.
func ParseKeyValues(input string, out map[string]string, sep string) error {
	if out == nil {
		return fmt.Errorf("'out is nil")
	}

	keyValues := ParseDedupLowercaseAndSortStrings(input, sep)
	if len(keyValues) == 0 {
		return nil
	}

	for _, keyValue := range keyValues {
		shards := strings.Split(keyValue, "=")
		if len(shards) != 2 {
			return fmt.Errorf("invalid <key,value> format")
		}

		key := strings.TrimSpace(shards[0])
		value := strings.TrimSpace(shards[1])
		if key == "" || value == "" {
			return fmt.Errorf("invalid <key,value> pair: key: %q value: %q", key, value)
		}
		out[key] = value
	}
	return nil
}

// ParseArbitraryKeyValues parses arbitrary <key,value> tuples. The input
// can be one of the following:
// * JSON string
// * Base64 encoded JSON string
// * Comma separated list of `<key>=<value>` pairs
// * Base64 encoded string containing comma separated list of
//   `<key>=<value>` pairs
//
// Input will be parsed into the output parameter, which should
// be a non-nil map[string]string.
func ParseArbitraryKeyValues(input string, out map[string]string, sep string) error {
	input = strings.TrimSpace(input)
	if input == "" {
		return nil
	}
	if out == nil {
		return fmt.Errorf("'out' is nil")
	}

	// Try to base64 decode the input. If successful, consider the decoded
	// value as input.
	inputBytes, err := base64.StdEncoding.DecodeString(input)
	if err == nil {
		input = string(inputBytes)
	}

	// Try to JSON unmarshal the input. If successful, consider that the
	// metadata was supplied as JSON input.
	err = json.Unmarshal([]byte(input), &out)
	if err != nil {
		// If JSON unmarshalling fails, consider that the input was
		// supplied as a comma separated string of 'key=value' pairs.
		if err = ParseKeyValues(input, out, sep); err != nil {
			return errwrap.Wrapf("failed to parse the input: {{err}}", err)
		}
	}

	// Validate the parsed input
	for key, value := range out {
		if key != "" && value == "" {
			return fmt.Errorf("invalid value for key %q", key)
		}
	}

	return nil
}

// ParseStringSlice parses a `sep`-separated list of strings into a
// []string with surrounding whitespace removed.
//
// The output will always be a valid slice but may be of length zero.
func ParseStringSlice(input string, sep string) []string {
	input = strings.TrimSpace(input)
	if input == "" {
		return []string{}
	}

	splitStr := strings.Split(input, sep)
	ret := make([]string, len(splitStr))
	for i, val := range splitStr {
		ret[i] = strings.TrimSpace(val)
	}

	return ret
}

// ParseArbitraryStringSlice parses arbitrary string slice. The input
// can be one of the following:
// * JSON string
// * Base64 encoded JSON string
// * `sep` separated list of values
// * Base64-encoded string containing a `sep` separated list of values
//
// Note that the separator is ignored if the input is found to already be in a
// structured format (e.g., JSON)
//
// The output will always be a valid slice but may be of length zero.
func ParseArbitraryStringSlice(input string, sep string) []string {
	input = strings.TrimSpace(input)
	if input == "" {
		return []string{}
	}

	// Try to base64 decode the input. If successful, consider the decoded
	// value as input.
	inputBytes, err := base64.StdEncoding.DecodeString(input)
	if err == nil {
		input = string(inputBytes)
	}

	ret := []string{}

	// Try to JSON unmarshal the input. If successful, consider that the
	// metadata was supplied as JSON input.
	err = json.Unmarshal([]byte(input), &ret)
	if err != nil {
		// If JSON unmarshalling fails, consider that the input was
		// supplied as a separated string of values.
		return ParseStringSlice(input, sep)
	}

	if ret == nil {
		return []string{}
	}

	return ret
}

// TrimStrings takes a slice of strings and returns a slice of strings
// with trimmed spaces
func TrimStrings(items []string) []string {
	ret := make([]string, len(items))
	for i, item := range items {
		ret[i] = strings.TrimSpace(item)
	}
	return ret
}

// RemoveDuplicates removes duplicate and empty elements from a slice of
// strings. This also may convert the items in the slice to lower case and
// returns a sorted slice.
func RemoveDuplicates(items []string, lowercase bool) []string {
	itemsMap := map[string]bool{}
	for _, item := range items {
		item = strings.TrimSpace(item)
		if lowercase {
			item = strings.ToLower(item)
		}
		if item == "" {
			continue
		}
		itemsMap[item] = true
	}
	items = make([]string, 0, len(itemsMap))
	for item := range itemsMap {
		items = append(items, item)
	}
	sort.Strings(items)
	return items
}

// EquivalentSlices checks whether the given string sets are equivalent, as in,
// they contain the same values.
func EquivalentSlices(a, b []string) bool {
	if a == nil && b == nil {
		return true
	}

	if a == nil || b == nil {
		return false
	}

	// First we'll build maps to ensure unique values
	mapA := map[string]bool{}
	mapB := map[string]bool{}
	for _, keyA := range a {
		mapA[keyA] = true
	}
	for _, keyB := range b {
		mapB[keyB] = true
	}

	// Now we'll build our checking slices
	var sortedA, sortedB []string
	for keyA := range mapA {
		sortedA = append(sortedA, keyA)
	}
	for keyB := range mapB {
		sortedB = append(sortedB, keyB)
	}
	sort.Strings(sortedA)
	sort.Strings(sortedB)

	// Finally, compare
	if len(sortedA) != len(sortedB) {
		return false
	}

	for i := range sortedA {
		if sortedA[i] != sortedB[i] {
			return false
		}
	}

	return true
}

// StrListDelete removes the first occurrence of the given item from the slice
// of strings if the item exists.
func StrListDelete(s []string, d string) []string {
	if s == nil {
		return s
	}

	for index, element := range s {
		if element == d {
			return append(s[:index], s[index+1:]...)
		}
	}

	return s
}

// GlobbedStringsMatch compares item to val with support for a leading and/or
// trailing wildcard '*' in item.
func GlobbedStringsMatch(item, val string) bool {
	if len(item) < 2 {
		return val == item
	}

	hasPrefix := strings.HasPrefix(item, "*")
	hasSuffix := strings.HasSuffix(item, "*")

	if hasPrefix && hasSuffix {
		return strings.Contains(val, item[1:len(item)-1])
	} else if hasPrefix {
		return strings.HasSuffix(val, item[1:])
	} else if hasSuffix {
		return strings.HasPrefix(val, item[:len(item)-1])
	}

	return val == item
}

// AppendIfMissing adds a string to a slice if the given string is not present
func AppendIfMissing(slice []string, i string) []string {
	if StrListContains(slice, i) {
		return slice
	}
	return append(slice, i)
}

// MergeSlices adds an arbitrary number of slices together, uniquely
func MergeSlices(args ...[]string) []string {
	all := map[string]struct{}{}
	for _, slice := range args {
		for _, v := range slice {
			all[v] = struct{}{}
		}
	}

	result := make([]string, 0, len(all))
	for k := range all {
		result = append(result, k)
	}
	sort.Strings(result)
	return result
}

// Difference returns the set difference (A - B) of the two given slices. The
// result will also remove any duplicated values in set A regardless of whether
// that matches any values in set B.
func Difference(a, b []string, lowercase bool) []string {
	if len(a) == 0 || len(b) == 0 {
		return a
	}

	a = RemoveDuplicates(a, lowercase)
	b = RemoveDuplicates(b, lowercase)

	itemsMap := map[string]bool{}
	for _, aVal := range a {
		itemsMap[aVal] = true
	}

	// Perform difference calculation
	for _, bVal := range b {
		if _, ok := itemsMap[bVal]; ok {
			itemsMap[bVal] = false
		}
	}

	items := []string{}
	for item, exists := range itemsMap {
		if exists {
			items = append(items, item)
		}
	}
	sort.Strings(items)
	return items
}
