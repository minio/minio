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

func TestCsv_AllowQuotedRecordDelimiter() []TestItem {
	return []TestItem{
		{
			Key:        "hello.csv",
			Content:    "Year,Make,Model\n1997,\"aaa\nbbb\",E350\n2000,Mercury,Cougar\n",
			Expression: "SELECT * FROM S3Object",
			Args: map[string]string{
				"InputSerialization_CSV_FileHeaderInfo":             "USE",
				"InputSerialization_CSV_AllowQuotedRecordDelimiter": "TRUE",
			},
			Expects: []Result{
				{
					Payload: "1997,\"aaa\nbbb\",E350\n2000,Mercury,Cougar\n",
				},
				{
					StatusCode: "400",
					ErrorCode:  "MalformedXML",
				},
			},
		},
	}
}

func TestCsv_Unnormal() []TestItem {
	return []TestItem{
		{
			Key:        "hello.csv",
			Content:    "Year,Make,Model\n1997,Ford,E350\n2000,Mercury,Cougar\n",
			Expression: "",
			Expects: []Result{
				{
					StatusCode: "400",
					ErrorCode:  "MissingRequiredParameter",
				},
				{
					StatusCode: "400",
					ErrorCode:  "ParseSelectFailure",
				},
			},
			Comment: "empty sql",
		},
		{
			Key:        "hello.csv",
			Content:    "Year,Make,Model\n1997,Ford,E350\n2000,Mercury,Cougar\n",
			Expression: "abc hahaha",
			Expects: []Result{
				{
					StatusCode: "400",
					ErrorCode:  "ParseUnexpectedToken",
				},
				{
					StatusCode: "400",
					ErrorCode:  "ParseSelectFailure",
				},
			},
			Comment: "abnormal sql",
		},
		{
			Key:        "hello.csv",
			Content:    "Year,Make,Model\n1997,Ford,E350\n2000,Mercury,Cougar\n",
			Expression: "select abc",
			Expects: []Result{
				{
					StatusCode: "400",
					ErrorCode:  "ParseSelectMissingFrom",
				},
				{
					StatusCode: "400",
					ErrorCode:  "ParseSelectFailure",
				},
			},
			Comment: "sql without where",
		},
		{
			Key:        "hello.csv",
			Content:    "Year,Make,Model\n1997,Ford,E350\n2000,Mercury,Cougar\n",
			Expression: "select abc where",
			Expects: []Result{
				{
					StatusCode: "400",
					ErrorCode:  "ParseSelectMissingFrom",
				},
				{
					StatusCode: "400",
					ErrorCode:  "ParseSelectFailure",
				},
			},
			Comment: "sql without from",
		},
		{
			Key:        "hello.csv",
			Content:    "Year,Make,Model\n1997,Ford,E350\n2000,Mercury,Cougar\n",
			Expression: "select abc where abc",
			Expects: []Result{
				{
					StatusCode: "400",
					ErrorCode:  "ParseSelectMissingFrom",
				},
				{
					StatusCode: "400",
					ErrorCode:  "ParseSelectFailure",
				},
			},
			Comment: "sql without from",
		},
		{
			Key:        "hello.csv",
			Content:    "Year,Make,Model\n1997,Ford,E350\n2000,Mercury,Cougar\n",
			Expression: "select abc from",
			Expects: []Result{
				{
					StatusCode: "400",
					ErrorCode:  "ParseUnexpectedTerm",
				},
				{
					StatusCode: "400",
					ErrorCode:  "ParseSelectFailure",
				},
			},
			Comment: "sql without table name",
		},
		{
			Key:        "hello.csv",
			Content:    "Year,Make,Model\n1997,Ford,E350\n2000,Mercury,Cougar\n",
			Expression: "select abc from 123",
			Expects: []Result{
				{
					StatusCode: "400",
					ErrorCode:  "InvalidDataSource",
				},
				{
					StatusCode: "400",
					ErrorCode:  "ParseSelectFailure",
				},
			},
			Comment: "sql with wrong table name",
		},
	}
}

func TestCsv_EmptyRecord() []TestItem {
	return []TestItem{
		{
			Key:        "hello.csv",
			Content:    "Year,Make,Model\n",
			Expression: "Select * from S3Object",
			Expects: []Result{
				{
					Payload: "",
				},
			},
		},
		{
			Key:        "hello.csv",
			Content:    "\n",
			Expression: "Select * from S3Object",
			Args: map[string]string{
				"InputSerialization_CSV_FileHeaderInfo": "NONE",
			},
			Expects: []Result{
				{
					Payload: "",
				},
			},
		},
	}
}

func TestCsv_SelectColumn_ColumnNumbers() []TestItem {
	return []TestItem{
		{
			Key:        "hello.csv",
			Content:    "Year,Make,Model\n1997,Ford,E350\n2000,Mercury,Cougar\n",
			Expression: "SELECT s._1 FROM S3Object s",
			Expects: []Result{
				{
					Payload: "1997\n2000\n",
				},
			},
		},
		{
			Key:        "hello.csv",
			Content:    "Year,Make,Model\n1997,Ford,E350\n2000,Mercury,Cougar\n",
			Expression: "SELECT s._4 FROM S3Object s",
			Expects: []Result{
				{
					Payload: "",
				},
				{
					StatusCode: "200",
					ErrorCode:  "InternalError: column _4 not found",
				},
			},
			Comment: "select nonexisted column",
		},
		{
			Key:        "hello.csv",
			Content:    "Year,Make,Model\n1997,Ford,E350\n2000,Mercury,Cougar\n",
			Expression: "SELECT s._0 FROM S3Object s",
			Expects: []Result{
				{
					StatusCode: "400",
					ErrorCode:  "InvalidColumnIndex",
				},
				{
					StatusCode: "200",
					ErrorCode:  "InternalError: column _0 not found",
				},
			},
			Comment: "select nonexisted column",
		},
	}
}

func TestCsv_SelectColumn_ColumnHeaders() []TestItem {
	return []TestItem{
		{
			Key:        "hello.csv",
			Content:    "name,age\nzhangshan,30\nlishi,28\n",
			Expression: "Select name from S3Object",
			Expects: []Result{
				{
					StatusCode: "400",
					ErrorCode:  "InvalidColumnIndex",
				},
				{
					StatusCode: "200",
					ErrorCode:  "InternalError: column name not found",
				},
			},
		},
	}
}

func TestCsv_SelectColumn_Where() []TestItem {
	return []TestItem{
		{
			Key:        "hello.csv",
			Content:    "name,age\nzhangshan,30\nlishi,28\n",
			Expression: "Select s._1 from S3Object s WHERE s._1='zhangshan'",
			Expects: []Result{
				{
					Payload: "zhangshan\n",
				},
			},
		},
		{
			Key:        "hello.csv",
			Content:    "name,age\nzhangshan,30\nlishi,28\n",
			Expression: "Select s._1 from S3Object s WHERE s._1!='zhangshan'",
			Expects: []Result{
				{
					Payload: "lishi\n",
				},
			},
		},
		{
			Key:        "hello.csv",
			Content:    "name,age\nzhangshan,30\nlishi,28\n",
			Expression: "Select s._1 from S3Object s WHERE s._1 >=  'zhangshan'",
			Expects: []Result{
				{
					Payload: "zhangshan\n",
				},
			},
		},
		{
			Key:        "hello.csv",
			Content:    "name,age\nzhangshan,30\nlishi,28\n",
			Expression: "Select s._1 from S3Object s WHERE s._1 >=  'lishi'",
			Expects: []Result{
				{
					Payload: "zhangshan\nlishi\n",
				},
			},
		},
		{
			Key:        "hello.csv",
			Content:    "name,age\nzhangshan,30\nlishi,28\n",
			Expression: "Select s._1 from S3Object s WHERE s._1 = 'abc'",
			Expects: []Result{
				{
					Payload: "",
				},
			},
		},
		{
			Key:        "hello.csv",
			Content:    "name,age\nzhangshan,30\nlishi,28\n",
			Expression: "Select s._1 from S3Object s WHERE s._1 = ''",
			Expects: []Result{
				{
					Payload: "",
				},
			},
		},
		{
			Key:        "hello.csv",
			Content:    "name,age\nzhangshan,30\nlishi,28\n",
			Expression: "Select s._1 from S3Object s WHERE cast(s._2 as int) < 29",
			Expects: []Result{
				{
					Payload: "lishi\n",
				},
			},
		},
		{
			Key:        "hello.csv",
			Content:    "name,age\nzhangshan,30\nlishi,28\n",
			Expression: "Select s._1 from S3Object s WHERE cast(s._2 as int) <= 28",
			Expects: []Result{
				{
					Payload: "lishi\n",
				},
			},
		},
		{
			Key:        "hello.csv",
			Content:    "name,age\nzhangshan,30\nlishi,28\n",
			Expression: "Select s._1 from S3Object s WHERE cast(s._2 as int) <= 100000",
			Expects: []Result{
				{
					Payload: "zhangshan\nlishi\n",
				},
			},
		},
		{
			Key:        "hello.csv",
			Content:    "name,age\nzhangshan,30\nlishi,28\n",
			Expression: "Select s._1 from S3Object s WHERE cast(s._2 as int) >= 0",
			Expects: []Result{
				{
					Payload: "zhangshan\nlishi\n",
				},
			},
		},
		{
			Key:        "hello.csv",
			Content:    "name,age\nzhangshan,30\nlishi,28\n",
			Expression: "Select s._1 from S3Object s WHERE cast(s._2 as int) != 28",
			Expects: []Result{
				{
					Payload: "zhangshan\n",
				},
			},
		},
	}
}

func TestCsv_SelectColumn_Limit() []TestItem {
	return []TestItem{
		{
			Key:        "hello.csv",
			Content:    "name,age\nzhangshan,30\nlishi,28\n",
			Expression: "Select s._1 from S3Object s limit 1",
			Expects: []Result{
				{
					Payload: "zhangshan\n",
				},
			},
		},
		{
			Key:        "hello.csv",
			Content:    "name,age\nzhangshan,30\nlishi,28\n",
			Expression: "Select s._1 from S3Object s limit 0",
			Expects: []Result{
				{
					Payload: "",
				},
			},
		},
		{
			Key:        "hello.csv",
			Content:    "name,age\nzhangshan,30\nlishi,28\n",
			Expression: "Select s._1 from S3Object s limit -1",
			Expects: []Result{
				{
					StatusCode: "400",
					ErrorCode:  "EvaluatorNegativeLimit",
				},
				{
					StatusCode: "400",
					ErrorCode:  "ParseSelectFailure",
				},
			},
		},
	}
}

func TestCsv_Quotes() []TestItem {
	return []TestItem{
		{
			Key:        "hello.csv",
			Content:    "\"name\",\"age\"\n\"zhangshan\",\"30\"\n\"lishi\",\"28\"\n",
			Expression: "Select s._1 from S3Object s limit 1",
			Expects: []Result{
				{
					Payload: "zhangshan\n",
				},
			},
			Comment: "",
		},
		{
			Key:        "hello.csv",
			Content:    "\"name\",\"a ge\"\n\"zhangshan\",\"30\"\n\"lishi\",\"28\"\n",
			Expression: "Select s._1 from S3Object s limit 1",
			Expects: []Result{
				{
					Payload: "zhangshan\n",
				},
			},
			Comment: "the column contains space",
		},
		{
			Key:        "hello.csv",
			Content:    "\"name\",\"age\"\n\"zhang shan\",\"30\"\n\"li shi\",\"28\"\n",
			Expression: "Select s._1 from S3Object s limit 1",
			Expects: []Result{
				{
					Payload: "zhang shan\n",
				},
			},
			Comment: "record contatins space",
		},
		{
			Key:        "hello.csv",
			Content:    "'name','age'\n'zhangshan','30'\n'lishi','28'\n",
			Expression: "Select s._1 from S3Object s limit 1",
			Expects: []Result{
				{
					Payload: "'zhangshan'\n",
				},
			},
			Comment: "single quote",
		},
		{
			Key:        "hello.csv",
			Content:    "'name','a ge'\n'zhangshan','30'\n'lishi','28'\n",
			Expression: "Select s._1 from S3Object s limit 1",
			Expects: []Result{
				{
					Payload: "'zhangshan'\n",
				},
			},
			Comment: "single quote",
		},
		{
			Key:        "hello.csv",
			Content:    "'name','age'\n'zhang shan','30'\n'li shi','28'\n",
			Expression: "Select s._1 from S3Object s limit 1",
			Expects: []Result{
				{
					Payload: "'zhang shan'\n",
				},
			},
			Comment: "single quote",
		},
		{
			Key:        "hello.csv",
			Content:    "name,age\nzhangshan,30\nlishi,28\n",
			Expression: "Select s._1 from S3Object s limit 1",
			Expects: []Result{
				{
					Payload: "zhangshan\n",
				},
			},
			Comment: "",
		},
		{
			Key:        "hello.csv",
			Content:    "name,a ge\nzhangshan,30\nlishi,28\n",
			Expression: "Select s._1 from S3Object s limit 1",
			Expects: []Result{
				{
					Payload: "zhangshan\n",
				},
			},
			Comment: "",
		},
		{
			Key:        "hello.csv",
			Content:    "name,age\nzhang shan,30\nli shi,28\n",
			Expression: "Select s._1 from S3Object s limit 1",
			Expects: []Result{
				{
					Payload: "zhang shan\n",
				},
			},
			Comment: "",
		},
	}
}

func TestCsv_Compression() []TestItem {
	return []TestItem{
		{
			Key:        "hello.csv",
			Content:    "Year,Make,Model\n1997,Ford,E350\n2000,Mercury,Cougar\n",
			Expression: "Select s._1 from S3Object s limit 1",
			Args: map[string]string{
				"InputSerialization_CompressionType": "GZIP",
			},
			Expects: []Result{
				{
					StatusCode: "400",
					ErrorCode:  "InvalidCompressionFormat",
				},
				{
					StatusCode: "400",
					ErrorCode:  "TruncatedInput",
				},
			},
		},
	}
}

func TestCsv_RecordDelimiter() []TestItem {
	return []TestItem{
		{
			Key:        "hello.csv",
			Content:    "Year,Make,Model\t1997,Ford,E350\t2000,Mercury,Cougar\t",
			Expression: "Select s._1 from S3Object s",
			Args: map[string]string{
				"InputSerialization_CSV_RecordDelimiter": "\t",
				"InputSerialization_CSV_FileHeaderInfo":  "NONE",
			},
			Expects: []Result{
				{
					Payload: "Year\n1997\n2000\n",
				},
			},
			Comment: "RecordDelimiter为\t",
		},
		{
			Key:        "hello.csv",
			Content:    "Year,Make,Model\n1997,Ford,E350\t2000,Mercury,Cougar\t",
			Expression: "Select s._1 from S3Object s",
			Args: map[string]string{
				"InputSerialization_CSV_RecordDelimiter": "\t",
				"InputSerialization_CSV_FileHeaderInfo":  "IGNORE",
			},
			Expects: []Result{
				{
					Payload: "2000\n",
				},
				{
					Payload: "1997\n2000\n",
				},
			},
		},
	}
}
