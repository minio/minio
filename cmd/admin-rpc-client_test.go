/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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
	"encoding/json"
	"reflect"
	"testing"
)

var (
	config1 = []byte(`{
	"version": "13",
	"credential": {
		"accessKey": "minio",
		"secretKey": "minio123"
	},
	"region": "us-east-1",
	"logger": {
		"console": {
			"enable": true,
			"level": "debug"
		},
		"file": {
			"enable": false,
			"fileName": "",
			"level": ""
		}
	},
	"notify": {
		"amqp": {
			"1": {
				"enable": false,
				"url": "",
				"exchange": "",
				"routingKey": "",
				"exchangeType": "",
				"mandatory": false,
				"immediate": false,
				"durable": false,
				"internal": false,
				"noWait": false,
				"autoDeleted": false
			}
		},
		"nats": {
			"1": {
				"enable": false,
				"address": "",
				"subject": "",
				"username": "",
				"password": "",
				"token": "",
				"secure": false,
				"pingInterval": 0,
				"streaming": {
					"enable": false,
					"clusterID": "",
					"clientID": "",
					"async": false,
					"maxPubAcksInflight": 0
				}
			}
		},
		"elasticsearch": {
			"1": {
				"enable": false,
				"url": "",
				"index": ""
			}
		},
		"redis": {
			"1": {
				"enable": false,
				"address": "",
				"password": "",
				"key": ""
			}
		},
		"postgresql": {
			"1": {
				"enable": false,
				"connectionString": "",
				"table": "",
				"host": "",
				"port": "",
				"user": "",
				"password": "",
				"database": ""
			}
		},
		"kafka": {
			"1": {
				"enable": false,
				"brokers": null,
				"topic": ""
			}
		},
		"webhook": {
			"1": {
				"enable": false,
				"endpoint": ""
			}
		}
	}
}
`)
	// diff from config1 - amqp.Enable is True
	config2 = []byte(`{
	"version": "13",
	"credential": {
		"accessKey": "minio",
		"secretKey": "minio123"
	},
	"region": "us-east-1",
	"logger": {
		"console": {
			"enable": true,
			"level": "debug"
		},
		"file": {
			"enable": false,
			"fileName": "",
			"level": ""
		}
	},
	"notify": {
		"amqp": {
			"1": {
				"enable": true,
				"url": "",
				"exchange": "",
				"routingKey": "",
				"exchangeType": "",
				"mandatory": false,
				"immediate": false,
				"durable": false,
				"internal": false,
				"noWait": false,
				"autoDeleted": false
			}
		},
		"nats": {
			"1": {
				"enable": false,
				"address": "",
				"subject": "",
				"username": "",
				"password": "",
				"token": "",
				"secure": false,
				"pingInterval": 0,
				"streaming": {
					"enable": false,
					"clusterID": "",
					"clientID": "",
					"async": false,
					"maxPubAcksInflight": 0
				}
			}
		},
		"elasticsearch": {
			"1": {
				"enable": false,
				"url": "",
				"index": ""
			}
		},
		"redis": {
			"1": {
				"enable": false,
				"address": "",
				"password": "",
				"key": ""
			}
		},
		"postgresql": {
			"1": {
				"enable": false,
				"connectionString": "",
				"table": "",
				"host": "",
				"port": "",
				"user": "",
				"password": "",
				"database": ""
			}
		},
		"kafka": {
			"1": {
				"enable": false,
				"brokers": null,
				"topic": ""
			}
		},
		"webhook": {
			"1": {
				"enable": false,
				"endpoint": ""
			}
		}
	}
}
`)
)

// TestGetValidServerConfig - test for getValidServerConfig.
func TestGetValidServerConfig(t *testing.T) {
	var c1, c2 serverConfig
	err := json.Unmarshal(config1, &c1)
	if err != nil {
		t.Fatalf("json unmarshal of %s failed: %v", string(config1), err)
	}

	err = json.Unmarshal(config2, &c2)
	if err != nil {
		t.Fatalf("json unmarshal of %s failed: %v", string(config2), err)
	}

	// Valid config.
	noErrs := []error{nil, nil, nil, nil}
	serverConfigs := []serverConfig{c1, c2, c1, c1}
	validConfig, err := getValidServerConfig(serverConfigs, noErrs)
	if err != nil {
		t.Errorf("Expected a valid config but received %v instead", err)
	}

	if !reflect.DeepEqual(validConfig, c1) {
		t.Errorf("Expected valid config to be %v but received %v", config1, validConfig)
	}

	// Invalid config - no quorum.
	serverConfigs = []serverConfig{c1, c2, c2, c1}
	_, err = getValidServerConfig(serverConfigs, noErrs)
	if err != errXLWriteQuorum {
		t.Errorf("Expected to fail due to lack of quorum but received %v", err)
	}

	// All errors
	allErrs := []error{errDiskNotFound, errDiskNotFound, errDiskNotFound, errDiskNotFound}
	serverConfigs = []serverConfig{{}, {}, {}, {}}
	_, err = getValidServerConfig(serverConfigs, allErrs)
	if err != errXLWriteQuorum {
		t.Errorf("Expected to fail due to lack of quorum but received %v", err)
	}
}
