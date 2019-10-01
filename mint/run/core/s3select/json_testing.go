/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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

func TestJson_Normal() []TestItem {
	return []TestItem{
		{
			Format: "JSON",
			Key:    "hello.json",
			Content: jsonPretty(`
{ 
    "name": "Susan Smith", 
    "org": "engineering",
    "projects": [
        {"project_name":"project1", "completed":false},
        {"project_name":"project2", "completed":true}
    ]
}`),
			Expression: "Select s.name from S3Object s",
			Expects: []Result{
				{
					Payload: `{"name":"Susan Smith"}
`,
				},
			},
		},
		{
			Format: "JSON",
			Key:    "hello.json",
			Content: `{"Year":"1997","Make":"Ford","Model":"E350"}
{"Year":"2000","Make":"Mercury","Model":"Cougar"}`,
			Expression: "Select * from S3Object s",
			Args: map[string]string{
				"InputSerialization_JSON_Type": "LINES",
			},
			Expects: []Result{
				{
					Payload: `{"Year":"1997","Make":"Ford","Model":"E350"}
{"Year":"2000","Make":"Mercury","Model":"Cougar"}
`,
				},
			},
		},
		{
			Format: "JSON",
			Key:    "hello.json",
			Content: `{"Year":"1997","Make":"Ford","Model":"E350"}
{"Year":"2000","Make":"Mercury","Model":"Cougar"}`,
			Expression: "Select * from S3Object s",
			Args: map[string]string{
				"InputSerialization_JSON_Type": "LINES",
				"OutputSerialization_CSV":      "true",
			},
			Expects: []Result{
				{
					Payload: `1997,Ford,E350
2000,Mercury,Cougar
`,
				},
			},
		},
		{
			Format: "JSON",
			Key:    "hello.json",
			Content: `{"Year":"1997","Make":"Ford","Model":"E350"}
{"Year":"2000","Make":"Mercury","Model":"Cougar"}`,
			Expression: "Select s.Make from S3Object s",
			Args: map[string]string{
				"InputSerialization_JSON_Type": "LINES",
				"OutputSerialization_CSV":      "true",
			},
			Expects: []Result{
				{
					Payload: `Ford
Mercury
`,
				},
			},
		},
	}
}
