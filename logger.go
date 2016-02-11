/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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

package main

import (
	"encoding/json"
	"reflect"

	"github.com/Sirupsen/logrus"
	"github.com/minio/minio/pkg/probe"
)

type fields map[string]interface{}

var log = logrus.New() // Default console logger.

func errorIf(err *probe.Error, msg string, fields map[string]interface{}) {
	if err == nil {
		return
	}
	if fields == nil {
		fields = make(map[string]interface{})
	}
	fields["Error"] = struct {
		Cause     string             `json:"cause,omitempty"`
		Type      string             `json:"type,omitempty"`
		CallTrace []probe.TracePoint `json:"trace,omitempty"`
		SysInfo   map[string]string  `json:"sysinfo,omitempty"`
	}{
		err.Cause.Error(),
		reflect.TypeOf(err.Cause).String(),
		err.CallTrace,
		err.SysInfo,
	}
	log.WithFields(fields).Error(msg)
}

func fatalIf(err *probe.Error, msg string, fields map[string]interface{}) {
	if err == nil {
		return
	}
	if fields == nil {
		fields = make(map[string]interface{})
	}

	fields["error"] = err.ToGoError()
	if jsonErr, e := json.Marshal(err); e == nil {
		fields["probe"] = string(jsonErr)
	}
	log.WithFields(fields).Fatal(msg)
}
