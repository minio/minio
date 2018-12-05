/*
 * Minio Cloud Storage, (C) 2016, 2017, 2018 Minio, Inc.
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

package ioutil

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

// Test for NormalizedCSVReader.
func TestNormalizedReader(t *testing.T) {
	inputs := []struct {
		inputcsv       string
		delimiter      string
		chunkSize      int
		quoteCharacter string
		quoteEscapeCharacter string
		expectedcsv string
	}{
		// case 1 - with default `\n` delimiter.
		{"username,age\nbanana,12\ncarrot,23\napple,34\nbrinjal,90\nraddish,45", "\n", 10, "\"", "\"","username,age\nbanana,12\ncarrot,23\napple,34\nbrinjal,90\nraddish,45"},
		// case 2 - with carriage return `\r` which should be replaced with `\n` by default.
		{"username,age\rbanana,12\rcarrot,23\rapple,34\rbrinjal,90\rraddish,45", "\n", 10, "\"", "\"", "username,age\nbanana,12\ncarrot,23\napple,34\nbrinjal,90\nraddish,45"},
		// case 3 - with a double character delimiter (octals).
		{"username,age\r\nbanana,12\r\ncarrot,23\r\napple,34\r\nbrinjal,90\r\nraddish,45", "\r\n", 10, "\"", "\"", "username,age\nbanana,12\ncarrot,23\napple,34\nbrinjal,90\nraddish,45"},
		// case 4 - with a double character delimiter.
		{"username,agexvbanana,12xvcarrot,23xvapple,34xvbrinjal,90xvraddish,45", "xv", 10, "\"", "\"", "username,age\nbanana,12\ncarrot,23\napple,34\nbrinjal,90\nraddish,45"},
		// case 5 - with a double character delimiter `\t `
		{"username,age\t banana,12\t carrot,23\t apple,34\t brinjal,90\t raddish,45", "\t ", 10, "\"", "\"", "username,age\nbanana,12\ncarrot,23\napple,34\nbrinjal,90\nraddish,45"},
		// case 6 - This is a special case where the first delimiter match falls in the 13'th byte space
		// ie, the last byte space of the read chunk, In this case the reader should peek in the next byte
		// and replace with `\n`.
		{"username,agexxbanana,12xxcarrot,23xxapple,34xxbrinjal,90xxraddish,45", "xx", 13, "\"", "\"", "username,age\nbanana,12\ncarrot,23\napple,34\nbrinjal,90\nraddish,45"},
		// case 7 - With default quote character
		{`username,age\n"banana",12\n"carrot",23\n"apple",34\n"brinjal",90\n"raddish",45`, "\n", 10, "\"", "\"", `username,age\n"banana",12\n"carrot",23\n"apple",34\n"brinjal",90\n"raddish",45`},
		// case 8 - With custom quote character
		{`username,age\n;banana;,12\n;carrot;,23\n;apple;,34\n;brinjal;,90\n;raddish;,45`, "\n", 10, ";", "\"", `username,age\n"banana",12\n"carrot",23\n"apple",34\n"brinjal",90\n"raddish",45`},
		// case 9 - With quoteEscapeCharacter - The value to use for escaping quotes inside value
		{`username,age\n"""ban"ana""",12\n"carrot",23\n"apple",34\n"brinjal",90\n"raddish",45`, "\n", 10, "\"", "\"", `username,age\n"""ban"ana""",12\n"carrot",23\n"apple",34\n"brinjal",90\n"raddish",45`},
		// case 10 - With a custom quoteEscapeCharacter and custom quotes
		{`username,age\n'''ban|anaa''',12\n|carrot|,23\n|apple|,34\n|brinjal|,90\n|raddish|,45`, "\n", 10, "|", "'", `username,age\n"""ban"anaa""",12\n"carrot",23\n"apple",34\n"brinjal",90\n"raddish",45`},

		{`username,age\n;banana";,12\n;carrot;,23\n;apple;,34\n;brinjal;,90\n;raddish;,45`, "\n", 10, ";", "\"", `username,age\n"banana;",12\n"carrot",23\n"apple",34\n"brinjal",90\n"raddish",45`},
		{`username,age\n'''ban|ana"''',12\n|carrot|,23\n|apple|,34\n|brinjal|,90\n|raddish|,45`, "\n", 10, "|", "'", `username,age\n"""ban"ana|""",12\n"carrot",23\n"apple",34\n"brinjal",90\n"raddish",45`},
		{`username,age\n"""ban"ana""",12\n"carrot",23\n"apple",34\n"brinjal",90\n"raddish",45`, "\n", 10, "\"", "\"", `username,age\n"""ban"ana""",12\n"carrot",23\n"apple",34\n"brinjal",90\n"raddish",45`},

		
	}

	for c, input := range inputs {
		var readcsv []byte
		var err error
		normalizedReader := NewNormalizedReader(strings.NewReader(input.inputcsv), []rune(input.delimiter), rune(input.quoteCharacter[0]), rune(input.quoteEscapeCharacter[0]))
		for err == nil {
			chunk := make([]byte, input.chunkSize)
			_, err = normalizedReader.Read(chunk)
			readcsv = append(readcsv, chunk...)
		}
		if err != io.EOF {
			t.Fatalf("Case %d: Error in normalized reader", c+1)
		}
		expected := []byte(input.expectedcsv)
		cleanCsv := removeNulls(readcsv)
		if !bytes.Equal(cleanCsv, expected) {
			t.Fatalf("Case %d: Expected the normalized csv to be `%s`, but instead found `%s`", c+1, string(expected), string(cleanCsv))
		}
	}

}

// Removes all the tailing nulls in chunks.
// Null chunks will be assigned if there is a reduction
// Eg, When `xv` is reduced to `\n`, the last byte is nullified.
func removeNulls(csv []byte) []byte {
	cleanCsv := []byte{}
	for _, p := range csv {
		if p != 0 {
			cleanCsv = append(cleanCsv, p)
		}
	}
	return cleanCsv
}
