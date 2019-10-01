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

func TestAccessLog_Normal() []TestItem {
	return []TestItem{
		{
			Key: "hello.csv",
			Content: `127.0.0.1 - - [17/Jun/2019:10:52:30 +0800] "GET / HTTP/1.1" 200 3700 "-" "curl/7.29.0" "-"
203.205.141.46 - - [17/Jun/2019:10:52:54 +0800] "GET / HTTP/1.1" 200 3700 "-" "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36" "-"
203.205.141.46 - - [17/Jun/2019:10:52:55 +0800] "GET / HTTP/1.1" 304 0 "-" "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36" "-"
203.205.141.46 - - [17/Jun/2019:10:52:59 +0800] "GET /abc HTTP/1.1" 404 3650 "-" "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36" "-"`,
			Expression: "Select s._1 from S3Object s",
			Args: map[string]string{
				"InputSerialization_CSV_FieldDelimiter": " ",
				"InputSerialization_CSV_FileHeaderInfo": "NONE",
			},
			Expects: []Result{
				{
					Payload: "127.0.0.1\n203.205.141.46\n203.205.141.46\n203.205.141.46\n",
				},
			},
			Comment: "",
		},
		{
			Key: "hello.csv",
			Content: `127.0.0.1 - - [17/Jun/2019:10:52:30 +0800] "GET / HTTP/1.1" 200 3700 "-" "curl/7.29.0" "-"
203.205.141.45 - - [17/Jun/2019:10:52:54 +0800] "GET / HTTP/1.1" 200 3700 "-" "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36" "-"
203.205.141.46 - - [17/Jun/2019:10:52:55 +0800] "GET / HTTP/1.1" 304 0 "-" "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36" "-"
203.205.141.47 - - [17/Jun/2019:10:52:59 +0800] "GET /abc HTTP/1.1" 404 3650 "-" "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36" "-"`,
			Expression: "Select s._1 from S3Object s where s._1 = '203.205.141.45'",
			Args: map[string]string{
				"InputSerialization_CSV_FieldDelimiter": " ",
				"InputSerialization_CSV_FileHeaderInfo": "NONE",
			},
			Expects: []Result{
				{
					Payload: "203.205.141.45\n",
				},
			},
			Comment: "filter by client ip",
		},
		{
			Key: "hello.csv",
			Content: `127.0.0.1 - - [17/Jun/2019:10:52:30 +0800] "GET / HTTP/1.1" 200 3700 "-" "curl/7.29.0" "-"
203.205.141.45 - - [17/Jun/2019:10:52:54 +0800] "GET / HTTP/1.1" 200 3700 "-" "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36" "-"
203.205.141.46 - - [17/Jun/2019:10:52:55 +0800] "GET / HTTP/1.1" 304 0 "-" "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36" "-"
203.205.141.47 - - [17/Jun/2019:10:52:59 +0800] "GET /abc HTTP/1.1" 404 3650 "-" "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36" "-"`,
			Expression: "Select * from S3Object s where s._1 = '203.205.141.45'",
			Args: map[string]string{
				"InputSerialization_CSV_FieldDelimiter": " ",
				"InputSerialization_CSV_FileHeaderInfo": "NONE",
			},
			Expects: []Result{
				{
					Payload: `203.205.141.45,-,-,[17/Jun/2019:10:52:54,+0800],GET / HTTP/1.1,200,3700,-,"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36",-` + "\n",
				},
			},
			Comment: "filter by client ip",
		},
		{
			Key: "hello.csv",
			Content: `127.0.0.1 - - [17/Jun/2019:10:52:30 +0800] "GET / HTTP/1.1" 200 3700 "-" "curl/7.29.0" "-"
203.205.141.45 - - [17/Jun/2019:10:52:54 +0800] "GET / HTTP/1.1" 200 3700 "-" "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36" "-"
203.205.141.46 - - [17/Jun/2019:10:52:55 +0800] "GET / HTTP/1.1" 304 0 "-" "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36" "-"
203.205.141.47 - - [17/Jun/2019:10:52:59 +0800] "GET /abc HTTP/1.1" 404 3650 "-" "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36" "-"`,
			Expression: "Select * from S3Object s where s._1 = '203.205.141.45'",
			Args: map[string]string{
				"InputSerialization_CSV_FieldDelimiter":  " ",
				"InputSerialization_CSV_FileHeaderInfo":  "NONE",
				"OutputSerialization_CSV_FieldDelimiter": " ",
			},
			Expects: []Result{
				{
					Payload: `203.205.141.45 - - [17/Jun/2019:10:52:54 +0800] "GET / HTTP/1.1" 200 3700 - "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36" -` + "\n",
				},
			},
			Comment: "a white space as the output delimiter",
		},
		{
			Key: "hello.csv",
			Content: `127.0.0.1 - - [17/Jun/2019:10:52:30 +0800] "GET / HTTP/1.1" 200 3700 "-" "curl/7.29.0" "-"
203.205.141.45 - - [17/Jun/2019:10:52:54 +0800] "GET / HTTP/1.1" 200 3700 "-" "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36" "-"
203.205.141.46 - - [17/Jun/2019:10:52:55 +0800] "GET / HTTP/1.1" 304 0 "-" "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36" "-"
203.205.141.47 - - [17/Jun/2019:10:52:59 +0800] "GET /abc HTTP/1.1" 404 3650 "-" "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36" "-"`,
			Expression: "Select * from S3Object s where s._1 like '203.205.141.%'",
			Args: map[string]string{
				"InputSerialization_CSV_FieldDelimiter": " ",
				"InputSerialization_CSV_FileHeaderInfo": "NONE",
			},
			Expects: []Result{
				{
					Payload: `203.205.141.45,-,-,[17/Jun/2019:10:52:54,+0800],GET / HTTP/1.1,200,3700,-,"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36",-
203.205.141.46,-,-,[17/Jun/2019:10:52:55,+0800],GET / HTTP/1.1,304,0,-,"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36",-
203.205.141.47,-,-,[17/Jun/2019:10:52:59,+0800],GET /abc HTTP/1.1,404,3650,-,"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36",-` + "\n",
				},
			},
			Comment: "filter by ip prefix",
		},
		{
			Key: "hello.csv",
			Content: `127.0.0.1 - - [17/Jun/2019:10:52:30 +0800] "GET / HTTP/1.1" 200 3700 "-" "curl/7.29.0" "-"
203.205.141.45 - - [17/Jun/2019:10:52:54 +0800] "GET / HTTP/1.1" 200 3700 "-" "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36" "-"
203.205.141.46 - - [17/Jun/2019:10:52:55 +0800] "GET / HTTP/1.1" 304 0 "-" "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36" "-"
203.205.141.47 - - [17/Jun/2019:10:52:59 +0800] "GET /abc HTTP/1.1" 404 3650 "-" "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36" "-"`,
			Expression: "Select * from S3Object s where s._10 like 'curl%'",
			Args: map[string]string{
				"InputSerialization_CSV_FieldDelimiter": " ",
				"InputSerialization_CSV_FileHeaderInfo": "NONE",
			},
			Expects: []Result{
				{
					Payload: `127.0.0.1,-,-,[17/Jun/2019:10:52:30,+0800],GET / HTTP/1.1,200,3700,-,curl/7.29.0,-` + "\n",
				},
			},
			Comment: "filter by user agent",
		},
		{
			Key: "hello.csv",
			Content: `127.0.0.1 - - [2019-06-17T11:57:49+08:00] "GET / HTTP/1.1" 200 3700 "-" "curl/7.29.0" "-"
203.205.141.45 - - [2019-06-18T11:57:49+08:00] "GET / HTTP/1.1" 200 3700 "-" "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36" "-"
203.205.141.46 - - [2019-06-18T11:57:49+08:00] "GET / HTTP/1.1" 304 0 "-" "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36" "-"
203.205.141.47 - - [2019-06-19T11:57:49+08:00] "GET /abc HTTP/1.1" 404 3650 "-" "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36" "-"`,
			Expression: "Select *  from S3Object s where CAST(TRIM(BOTH '[]' FROM s._4) AS TIMESTAMP ) >= CAST ('2019-06-18T00:00:00+08:00' AS TIMESTAMP) AND CAST(TRIM(BOTH '[]' FROM s._4) AS TIMESTAMP ) < CAST('2019-06-19T00:00:00+08:00' AS TIMESTAMP) ",
			Args: map[string]string{
				"InputSerialization_CSV_FieldDelimiter": " ",
				"InputSerialization_CSV_FileHeaderInfo": "NONE",
			},
			Expects: []Result{
				{
					Payload: `203.205.141.45,-,-,[2019-06-18T11:57:49+08:00],GET / HTTP/1.1,200,3700,-,"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36",-
203.205.141.46,-,-,[2019-06-18T11:57:49+08:00],GET / HTTP/1.1,304,0,-,"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36",-` + "\n",
				},
			},
			Comment: "filter by time range",
		},
	}
}
