// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package k8s

import (
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sort"

	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/pkg/env"
)

// k8s iam store environment values
const (
	IamStoreEnabled = "k8s_iam_store_enabled"
	IamStoreNamespace     = "k8s_iam_store_namespace"
	IamStoreConfigMapName    = "k8s_iam_store_config_map_name"
	KubeConfigPath   = "k8s_kube_config_path"

	EnvIamStoreEnabled = "MINIO_K8S_IAM_STORE_ENABLED"
	EnvIamStoreNamespace     = "MINIO_K8S_IAM_STORE_NAMESPACE"
	EnvIamStoreConfigMapName    = "MINIO_K8S_IAM_STORE_CONFIG_MAP_NAME"
	EnvKubeConfigPath   = "MINIO_K8S_KUBE_CONFIG_PATH"
)

// DefaultKVS - default KV settings for k8s iam store.
var (
	DefaultKVS = config.KVS{
		config.KV{
			Key:   IamStoreEnabled,
			Value: "",
		},
		config.KV{
			Key:   IamStoreNamespace,
			Value: "default",
		},
		config.KV{
			Key:   IamStoreConfigMapName,
			Value: "minio-k8s-iam-store",
		},
		config.KV{
			Key: KubeConfigPath,
			Value: "",
		},
	}
)

// Config - server k8s config.
type Config struct {
	Enabled     bool   `json:"enabled"`
	Namespace  string `json:"namespace"`
	ConfigMapName string `json:"configMapName"`
	KubeConfigPath string `json:"kubeConfigPath"`
}

// New - initialize new k8s clientset.
func New(cfg Config) (*kubernetes.Clientset, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	var k8sconfig *rest.Config
	var err error
	if cfg.KubeConfigPath != "" {
		k8sconfig, err = clientcmd.BuildConfigFromFlags("", cfg.KubeConfigPath)
		if err != nil {
			return nil, err
		}
	} else {
		k8sconfig, err = rest.InClusterConfig()
		if err != nil {
			return nil, err
		}
	}
	clientset, err := kubernetes.NewForConfig(k8sconfig)
	if err != nil {
		return nil, err
	}
	return clientset, nil
}

func k8sIamStoreIsEnabled(enableValue string) bool {
	enabledValues := []string{"1", "true", "True"}
	return contains(enabledValues, enableValue)
}

// Enabled returns if k8s is enabled.
func Enabled(kvs config.KVS) bool {
	endpoints := kvs.Get(IamStoreEnabled)
	return endpoints != ""
}

func contains(s []string, searchterm string) bool {
	i := sort.SearchStrings(s, searchterm)
	return i < len(s) && s[i] == searchterm
}

// LookupConfig - Initialize new k8s config.
func LookupConfig(kvs config.KVS) (Config, error) {
	cfg := Config{}
	if err := config.CheckValidKeys(config.K8sSubSys, kvs, DefaultKVS); err != nil {
		return cfg, err
	}

	enabled := k8sIamStoreIsEnabled(env.Get(EnvIamStoreEnabled, kvs.Get(IamStoreEnabled)))
	if !enabled {
		return cfg, nil
	}

	cfg.Enabled = enabled
	cfg.Namespace = env.Get(EnvIamStoreNamespace, kvs.Get(IamStoreNamespace))
	cfg.ConfigMapName = env.Get(EnvIamStoreConfigMapName, kvs.Get(IamStoreConfigMapName))
	cfg.KubeConfigPath = env.Get(EnvKubeConfigPath, kvs.Get(KubeConfigPath))

	return cfg, nil
}
