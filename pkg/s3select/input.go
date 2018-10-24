/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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

package s3select

import (
	"bytes"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/minio/minio/pkg/s3select/format"
	"github.com/minio/minio/pkg/s3select/format/csv"
	"github.com/minio/minio/pkg/s3select/format/json"
)

const (
	// progressTime is the time interval for which a progress message is sent.
	progressTime time.Duration = 60 * time.Second
	// continuationTime is the time interval for which a continuation message is
	// sent.
	continuationTime time.Duration = 5 * time.Second
)

// ParseSelectTokens tokenizes the select query into required Columns, Alias, limit value
// where clause, aggregate functions, myFunctions, error.
type ParseSelectTokens struct {
	reqCols          []string
	alias            string
	myLimit          int64
	whereClause      interface{}
	aggFunctionNames []string
	myFuncs          *SelectFuncs
	myErr            error
}

// Row is a Struct for keeping track of key aspects of a row.
type Row struct {
	record string
	err    error
}

// This function replaces "",'' with `` for the select parser
func cleanExpr(expr string) string {
	r := strings.NewReplacer("\"", "`", "'", "`")
	return r.Replace(expr)
}

// New - initialize new select format
func New(gr io.Reader, size int64, req ObjectSelectRequest) (s3s format.Select, err error) {
	//  Initializating options for CSV
	if req.InputSerialization.CSV != nil {
		if req.OutputSerialization.CSV.FieldDelimiter == "" {
			req.OutputSerialization.CSV.FieldDelimiter = ","
		}
		if req.InputSerialization.CSV.FileHeaderInfo == "" {
			req.InputSerialization.CSV.FileHeaderInfo = CSVFileHeaderInfoNone
		}
		if req.InputSerialization.CSV.RecordDelimiter == "" {
			req.InputSerialization.CSV.RecordDelimiter = "\n"
		}
		s3s, err = csv.New(&csv.Options{
			HasHeader:            req.InputSerialization.CSV.FileHeaderInfo != CSVFileHeaderInfoNone,
			RecordDelimiter:      req.InputSerialization.CSV.RecordDelimiter,
			FieldDelimiter:       req.InputSerialization.CSV.FieldDelimiter,
			Comments:             req.InputSerialization.CSV.Comments,
			Name:                 "S3Object", // Default table name for all objects
			ReadFrom:             gr,
			Compressed:           string(req.InputSerialization.CompressionType),
			Expression:           cleanExpr(req.Expression),
			OutputFieldDelimiter: req.OutputSerialization.CSV.FieldDelimiter,
			StreamSize:           size,
			HeaderOpt:            req.InputSerialization.CSV.FileHeaderInfo == CSVFileHeaderInfoUse,
			Progress:             req.RequestProgress.Enabled,
		})
	} else if req.InputSerialization.JSON != nil {
		//  Initializating options for JSON
		s3s, err = json.New(&json.Options{
			Name:       "S3Object", // Default table name for all objects
			ReadFrom:   gr,
			Compressed: string(req.InputSerialization.CompressionType),
			Expression: cleanExpr(req.Expression),
			StreamSize: size,
			Type:       req.InputSerialization.JSON.Type == JSONTypeDocument,
			Progress:   req.RequestProgress.Enabled,
		})
	}
	return s3s, err
}

// Execute is the function where all the blocking occurs, It writes to the HTTP
// response writer in a streaming fashion so that the client can actively use
// the results before the query is finally finished executing. The
func Execute(writer io.Writer, f format.Select) error {
	myRow := make(chan *Row)
	curBuf := bytes.NewBuffer(make([]byte, 1000000))
	curBuf.Reset()
	progressTicker := time.NewTicker(progressTime)
	continuationTimer := time.NewTimer(continuationTime)
	defer progressTicker.Stop()
	defer continuationTimer.Stop()

	go runSelectParser(f, myRow)

	for {
		select {
		case row, ok := <-myRow:
			if ok && row.err != nil {
				errorMessage := writeErrorMessage(row.err, curBuf)
				_, err := errorMessage.WriteTo(writer)
				flusher, okFlush := writer.(http.Flusher)
				if okFlush {
					flusher.Flush()
				}
				if err != nil {
					return err
				}
				curBuf.Reset()
				close(myRow)
				return nil
			} else if ok {
				message := writeRecordMessage(row.record, curBuf)
				_, err := message.WriteTo(writer)
				flusher, okFlush := writer.(http.Flusher)
				if okFlush {
					flusher.Flush()
				}
				if err != nil {
					return err
				}
				curBuf.Reset()
				f.UpdateBytesReturned(int64(len(row.record)))
				if !continuationTimer.Stop() {
					<-continuationTimer.C
				}
				continuationTimer.Reset(continuationTime)
			} else if !ok {
				statPayload, err := f.CreateStatXML()
				if err != nil {
					return err
				}
				statMessage := writeStatMessage(statPayload, curBuf)
				_, err = statMessage.WriteTo(writer)
				flusher, ok := writer.(http.Flusher)
				if ok {
					flusher.Flush()
				}
				if err != nil {
					return err
				}
				curBuf.Reset()
				message := writeEndMessage(curBuf)
				_, err = message.WriteTo(writer)
				flusher, ok = writer.(http.Flusher)
				if ok {
					flusher.Flush()
				}
				if err != nil {
					return err
				}
				return nil
			}

		case <-progressTicker.C:
			// Send progress messages only if requested by client.
			if f.Progress() {
				progressPayload, err := f.CreateProgressXML()
				if err != nil {
					return err
				}
				progressMessage := writeProgressMessage(progressPayload, curBuf)
				_, err = progressMessage.WriteTo(writer)
				flusher, ok := writer.(http.Flusher)
				if ok {
					flusher.Flush()
				}
				if err != nil {
					return err
				}
				curBuf.Reset()
			}
		case <-continuationTimer.C:
			message := writeContinuationMessage(curBuf)
			_, err := message.WriteTo(writer)
			flusher, ok := writer.(http.Flusher)
			if ok {
				flusher.Flush()
			}
			if err != nil {
				return err
			}
			curBuf.Reset()
			continuationTimer.Reset(continuationTime)
		}
	}
}
