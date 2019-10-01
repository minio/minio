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

func TestSqlFunctions_Normal() []TestItem {
	return []TestItem{
		{
			Format:     "CSV",
			Key:        "hello.csv",
			Content:    "Name,Score\nalice,80\nbob,81\n",
			Expression: "SELECT AVG(Score) FROM S3Object",
			Args: map[string]string{
				"InputSerialization_CSV_FileHeaderInfo": "USE",
			},
			Expects: []Result{
				{
					Payload:    "",
					StatusCode: "400",
					ErrorCode:  "ExternalEvalException",
				},
				{
					Payload: "80.5\n", // MinIO feature auto detect the type
				},
			},
		},
		{
			Format:     "CSV",
			Key:        "hello.csv",
			Content:    "Name,Score\nalice,80\nbob,81\n",
			Expression: "SELECT AVG(CAST (Score as int)) FROM S3Object",
			Args: map[string]string{
				"InputSerialization_CSV_FileHeaderInfo": "USE",
			},
			Expects: []Result{
				{
					Payload: "80.5\n",
				},
			},
		},
		{
			Format:     "CSV",
			Key:        "hello.csv",
			Content:    "Name,Score\nalice,80\nbob,81\n",
			Expression: "SELECT SUM(CAST (Score as int)) FROM S3Object",
			Args: map[string]string{
				"InputSerialization_CSV_FileHeaderInfo": "USE",
			},
			Expects: []Result{
				{
					Payload: "161\n",
				},
			},
		},
		{
			Format:     "CSV",
			Key:        "hello.csv",
			Content:    "Name,Score\nalice,80\nbob,81\n",
			Expression: "SELECT MAX(CAST (Score as int)) FROM S3Object",
			Args: map[string]string{
				"InputSerialization_CSV_FileHeaderInfo": "USE",
			},
			Expects: []Result{
				{
					Payload: "81\n",
				},
			},
		},
		{
			Format:     "CSV",
			Key:        "hello.csv",
			Content:    "Name,Score\nalice,80\nbob,81\n",
			Expression: "SELECT MIN(CAST (Score as int)) FROM S3Object",
			Args: map[string]string{
				"InputSerialization_CSV_FileHeaderInfo": "USE",
			},
			Expects: []Result{
				{
					Payload: "80\n",
				},
			},
		},
		{
			Format:     "CSV",
			Key:        "hello.csv",
			Content:    "Name,Score\nalice,80\nbob,81\n",
			Expression: "SELECT COUNT(*) FROM S3Object",
			Args: map[string]string{
				"InputSerialization_CSV_FileHeaderInfo": "USE",
			},
			Expects: []Result{
				{
					Payload: "2\n",
				},
			},
		},
		{
			Format:     "CSV",
			Key:        "hello.csv",
			Content:    "Name,Score\nalice,80\nbob,81\n",
			Expression: "SELECT COUNT(Score) FROM S3Object",
			Args: map[string]string{
				"InputSerialization_CSV_FileHeaderInfo": "USE",
			},
			Expects: []Result{
				{
					Payload: "2\n",
				},
			},
		},
		{
			Format:     "CSV",
			Key:        "hello.csv",
			Content:    "Name,Score\n张三,80\nbob,81\n",
			Expression: "SELECT CHAR_LENGTH(Name) FROM S3Object",
			Args: map[string]string{
				"InputSerialization_CSV_FileHeaderInfo": "USE",
			},
			Expects: []Result{
				{
					Payload: "2\n3\n",
				},
			},
		},
		{
			Format:     "CSV",
			Key:        "hello.csv",
			Content:    "Name,Score\n张三,80\nbob,81\n",
			Expression: "SELECT CHARACTER_LENGTH(Name) FROM S3Object",
			Args: map[string]string{
				"InputSerialization_CSV_FileHeaderInfo": "USE",
			},
			Expects: []Result{
				{
					Payload: "2\n3\n",
				},
			},
		},
		{
			Format:     "CSV",
			Key:        "hello.csv",
			Content:    "Name,Score\n张三,80\nBob,81\n",
			Expression: "SELECT LOWER(Name) FROM S3Object",
			Args: map[string]string{
				"InputSerialization_CSV_FileHeaderInfo": "USE",
			},
			Expects: []Result{
				{
					Payload: "张三\nbob\n",
				},
			},
		},
		{
			Format:     "CSV",
			Key:        "hello.csv",
			Content:    "Name,Score\n张三,80\nBob,81\n",
			Expression: "SELECT SUBSTRING(Name, 0) FROM S3Object",
			Args: map[string]string{
				"InputSerialization_CSV_FileHeaderInfo": "USE",
			},
			Expects: []Result{
				{
					Payload: "张三\nBob\n",
				},
			},
		},
		{
			Format:     "CSV",
			Key:        "hello.csv",
			Content:    "Name,Score\n张三,80\nBob,81\n",
			Expression: "SELECT SUBSTRING(Name, 1) FROM S3Object",
			Args: map[string]string{
				"InputSerialization_CSV_FileHeaderInfo": "USE",
			},
			Expects: []Result{
				{
					Payload: "张三\nBob\n",
				},
			},
		},
		{
			Format:     "CSV",
			Key:        "hello.csv",
			Content:    "Name,Score\n张三,80\nBob,81\n",
			Expression: "SELECT SUBSTRING(Name, 2) FROM S3Object",
			Args: map[string]string{
				"InputSerialization_CSV_FileHeaderInfo": "USE",
			},
			Expects: []Result{
				{
					Payload: "三\nob\n",
				},
			},
		},
		{
			Format:     "CSV",
			Key:        "hello.csv",
			Content:    "Name,Score\n张三,80\nBobbbbbbb,81\n",
			Expression: "SELECT SUBSTRING(Name, 2, 3) FROM S3Object",
			Args: map[string]string{
				"InputSerialization_CSV_FileHeaderInfo": "USE",
			},
			Expects: []Result{
				{
					Payload: "三\nobb\n",
				},
			},
		},
		{
			Format:     "CSV",
			Key:        "hello.csv",
			Content:    "Name,Score\n张三,80\nBobbbbbbb,81\n",
			Expression: "SELECT SUBSTRING(Name, 100, 100) FROM S3Object",
			Args: map[string]string{
				"InputSerialization_CSV_FileHeaderInfo": "USE",
			},
			Expects: []Result{
				{
					Payload: "",
				},
			},
		},
		{
			Format:     "CSV",
			Key:        "hello.csv",
			Content:    "Name,Score\n  ,80\n,81\n",
			Expression: "SELECT SUBSTRING(Name, 1, 1) FROM S3Object",
			Args: map[string]string{
				"InputSerialization_CSV_FileHeaderInfo": "USE",
			},
			Expects: []Result{
				{
					Payload: "\" \"\n",
				},
			},
		},
		{
			Format:     "CSV",
			Key:        "hello.csv",
			Content:    "Name,Score\n  张三   ,80\n  Bobbbbbbb   ,81\n",
			Expression: "SELECT trim(Name) FROM S3Object",
			Args: map[string]string{
				"InputSerialization_CSV_FileHeaderInfo": "USE",
			},
			Expects: []Result{
				{
					Payload: "张三\nBobbbbbbb\n",
				},
			},
		},
		{
			Format:     "CSV",
			Key:        "hello.csv",
			Content:    "Name,Score\n  张三   ,80\n  Bobbbbbbb   ,81\n",
			Expression: "SELECT trim(LEADING FROM Name) FROM S3Object",
			Args: map[string]string{
				"InputSerialization_CSV_FileHeaderInfo": "USE",
			},
			Expects: []Result{
				{
					Payload: "张三   \nBobbbbbbb   \n",
				},
			},
		},
		{
			Format:     "CSV",
			Key:        "hello.csv",
			Content:    "Name,Score\n  张三   ,80\n  Bobbbbbbb   ,81\n",
			Expression: "SELECT trim(TRAILING  FROM Name) FROM S3Object",
			Args: map[string]string{
				"InputSerialization_CSV_FileHeaderInfo": "USE",
			},
			Expects: []Result{
				{
					Payload: "\"  张三\"\n\"  Bobbbbbbb\"\n",
				},
			},
		},
		{
			Format:     "CSV",
			Key:        "hello.csv",
			Content:    "Name,Score\n  张三   ,80\n  Bobbbbbbb   ,81\n",
			Expression: "SELECT trim(BOTH  FROM Name) FROM S3Object",
			Args: map[string]string{
				"InputSerialization_CSV_FileHeaderInfo": "USE",
			},
			Expects: []Result{
				{
					Payload: "张三\nBobbbbbbb\n",
				},
			},
		},
		{
			Format:     "CSV",
			Key:        "hello.csv",
			Content:    "Name,Score\n   张三   ,80\n  Bobbbbbbb,81\n",
			Expression: "SELECT trim(BOTH 'b' FROM Name) FROM S3Object",
			Args: map[string]string{
				"InputSerialization_CSV_FileHeaderInfo": "USE",
			},
			Expects: []Result{
				{
					Payload: "\"   张三   \"\n\"  Bo\"\n",
				},
			},
		},
		{
			Format:     "CSV",
			Key:        "hello.csv",
			Content:    "Name,Score\n张三张张,80\n  Bobbbbbbb,81\n",
			Expression: "SELECT trim(BOTH '张' FROM Name) FROM S3Object",
			Args: map[string]string{
				"InputSerialization_CSV_FileHeaderInfo": "USE",
			},
			Expects: []Result{
				{
					Payload: "三\n\"  Bobbbbbbb\"\n",
				},
			},
		},
		{
			Format:     "CSV",
			Key:        "hello.csv",
			Content:    "Name,Score\n张三张张,80\n  Bobbbbbbb,81\n",
			Expression: "SELECT trim(BOTH '张b' FROM Name) FROM S3Object",
			Args: map[string]string{
				"InputSerialization_CSV_FileHeaderInfo": "USE",
			},
			Expects: []Result{
				{
					Payload: "三\n\"  Bo\"\n",
				},
			},
		},
		{
			Format:     "CSV",
			Key:        "hello.csv",
			Content:    "Name,Score\n张三,80\n  Bobbbbbbb,81\n",
			Expression: "SELECT UPPER(Name) FROM S3Object",
			Args: map[string]string{
				"InputSerialization_CSV_FileHeaderInfo": "USE",
			},
			Expects: []Result{
				{
					Payload: "张三\n\"  BOBBBBBBB\"\n",
				},
			},
		},
	}
}
