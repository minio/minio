package etcd

import "strings"

// Prefix for all etcd records.
var GlobalEtcdPrefixPath string

func EtcdPath(key string) string {
	if GlobalEtcdPrefixPath == "" || strings.HasPrefix(key, GlobalEtcdPrefixPath) {
		return key
	}
	if strings.HasPrefix(key, "/") {
		return GlobalEtcdPrefixPath + key
	} else {
		return GlobalEtcdPrefixPath + "/" + key
	}
}
