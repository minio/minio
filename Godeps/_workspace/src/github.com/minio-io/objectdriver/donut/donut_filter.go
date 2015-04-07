package donut

import (
	"bufio"
	"bytes"
	"strings"

	"github.com/minio-io/objectdriver"
)

func delimiter(object, delimiter string) string {
	readBuffer := bytes.NewBufferString(object)
	reader := bufio.NewReader(readBuffer)
	stringReader := strings.NewReader(delimiter)
	delimited, _ := stringReader.ReadByte()
	delimitedStr, _ := reader.ReadString(delimited)
	return delimitedStr
}

func appendUniq(slice []string, i string) []string {
	for _, ele := range slice {
		if ele == i {
			return slice
		}
	}
	return append(slice, i)
}

func (d donutDriver) filter(objects []string, resources drivers.BucketResourcesMetadata) ([]string, []string) {
	var actualObjects []string
	var commonPrefixes []string
	for _, name := range objects {
		switch true {
		// Both delimiter and Prefix is present
		case resources.IsDelimiterPrefixSet():
			if strings.HasPrefix(name, resources.Prefix) {
				trimmedName := strings.TrimPrefix(name, resources.Prefix)
				delimitedName := delimiter(trimmedName, resources.Delimiter)
				if delimitedName != "" {
					if delimitedName == resources.Delimiter {
						commonPrefixes = appendUniq(commonPrefixes, resources.Prefix+delimitedName)
					} else {
						commonPrefixes = appendUniq(commonPrefixes, delimitedName)
					}
					if trimmedName == delimitedName {
						actualObjects = appendUniq(actualObjects, name)
					}
				}
			}
			// Delimiter present and Prefix is absent
		case resources.IsDelimiterSet():
			delimitedName := delimiter(name, resources.Delimiter)
			switch true {
			case delimitedName == name:
				actualObjects = appendUniq(actualObjects, name)
			case delimitedName != "":
				commonPrefixes = appendUniq(commonPrefixes, delimitedName)
			}
		case resources.IsPrefixSet():
			if strings.HasPrefix(name, resources.Prefix) {
				actualObjects = appendUniq(actualObjects, name)
			}
		case resources.IsDefault():
			return objects, nil
		}
	}
	return actualObjects, commonPrefixes
}
