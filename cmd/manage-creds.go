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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/minio/minio/pkg/auth"
)

//load save add getCredbyAccessToken getCredbyAccessKey

//Global Map of credentialSts vars stored in keys.json
var globalCreds map[string]credentialSts = make(map[string]credentialSts)

type credentialSts struct {
	AccessKey    string  `json:"accessKey"`
	SecretKey    string  `json:"secretKey"`
	ExpTime      float64 `json:"expTime"`
	SessionToken string  `json:"sessionToken"`
}

// Read file from keys.json stored in ~/.minio into globalCreds map
func loadCredentialMap() error {
	content, err := ioutil.ReadFile(getKeysFile())
	if err != nil {
		return err
	}
	json.Unmarshal(content, &globalCreds)
	return nil
}

// Write globalCreds map into keys.json
func saveCredentialMap() error {
	b, err := json.MarshalIndent(globalCreds, "", "    ")
	if err != nil {
		fmt.Printf("Error is %v\n", err)
	}

	//keysWrite := ioutil.WriteFile("/Users/sanatmouli/.minio/keys.json", b, 0644)
	keysWrite := ioutil.WriteFile(getKeysFile(), b, 0644)
	if keysWrite != nil {
		fmt.Printf("Error is %v\n", err)
	}
	return nil
}

// Add a new credential to the globalCreds Map
func addToCredentialMap(cred auth.Credentials, timeValid float64) error {
	authcred := &credentialSts{
		AccessKey:    cred.AccessKey,
		SecretKey:    cred.SecretKey,
		ExpTime:      timeValid,
		SessionToken: "",
	}
	fmt.Printf("AccessKey: %s SecretKey: %s ExpirationTime: %f\n", authcred.AccessKey, authcred.SecretKey, authcred.ExpTime)
	globalCreds[cred.AccessKey] = *authcred
	err := saveCredentialMap()
	if err != nil {
		fmt.Printf("Error is %v\n", err)
	}

	return nil

}

func deleteFromCredentialMap(accessKey string) {
	delete(globalCreds, accessKey)
}

// func getCredentialByAccessToken() error {

// }

// func getCredentialByAccessKey() error {

// }

func purgeExpiredKeys() {
	for {
		loadCredentialMap()
		for k := range globalCreds {
			fmt.Println("KEY IS ", k)
			if globalCreds[k].ExpTime < float64(time.Now().Unix()) {
				delete(globalCreds, k)
			}
		}
		time.Sleep(10000 * time.Millisecond)
		saveCredentialMap()
	}
}
