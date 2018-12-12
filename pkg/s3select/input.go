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
	"compress/bzip2"
	"io"
	"net/http"
	"strings"
	"time"

	humanize "github.com/dustin/go-humanize"
	"github.com/klauspost/pgzip"

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

// Row is a Struct for keeping track of key aspects of a row.
type Row struct {
	record string
	err    error
}

// This function replaces "",'' with `` for the select parser
func cleanExpr(expr string) string {
	r := strings.NewReplacer("\"", "`")
	return r.Replace(expr)
}

// New - initialize new select format
func New(reader io.Reader, size int64, req ObjectSelectRequest) (s3s format.Select, err error) {
	switch req.InputSerialization.CompressionType {
	case SelectCompressionGZIP:
		if reader, err = pgzip.NewReader(reader); err != nil {
			return nil, format.ErrTruncatedInput
		}
	case SelectCompressionBZIP:
		reader = bzip2.NewReader(reader)
	}

	//  Initializating options for CSV
	if req.InputSerialization.CSV != nil {
		if req.InputSerialization.CSV.FileHeaderInfo == "" {
			req.InputSerialization.CSV.FileHeaderInfo = CSVFileHeaderInfoNone
		}
		if req.InputSerialization.CSV.RecordDelimiter == "" {
			req.InputSerialization.CSV.RecordDelimiter = "\n"
		}
		options := &csv.Options{
			Name:            "S3Object", // Default table name for all objects
			HasHeader:       req.InputSerialization.CSV.FileHeaderInfo == CSVFileHeaderInfoUse,
			RecordDelimiter: req.InputSerialization.CSV.RecordDelimiter,
			FieldDelimiter:  req.InputSerialization.CSV.FieldDelimiter,
			Comments:        req.InputSerialization.CSV.Comments,
			ReadFrom:        reader,
			Compressed:      string(req.InputSerialization.CompressionType),
			Expression:      cleanExpr(req.Expression),
			StreamSize:      size,
			HeaderOpt:       req.InputSerialization.CSV.FileHeaderInfo == CSVFileHeaderInfoUse,
			Progress:        req.RequestProgress.Enabled,
		}
		if req.OutputSerialization.CSV != nil {
			if req.OutputSerialization.CSV.FieldDelimiter == "" {
				req.OutputSerialization.CSV.FieldDelimiter = ","
			}
			options.OutputFieldDelimiter = req.OutputSerialization.CSV.FieldDelimiter
			options.OutputRecordDelimiter = req.OutputSerialization.CSV.RecordDelimiter
			options.OutputType = format.CSV
		}
		if req.OutputSerialization.JSON != nil {
			options.OutputRecordDelimiter = req.OutputSerialization.JSON.RecordDelimiter
			options.OutputType = format.JSON
		}
		// Initialize CSV input type
		s3s, err = csv.New(options)
	} else if req.InputSerialization.JSON != nil {
		options := &json.Options{
			Name:         "S3Object", // Default table name for all objects
			ReadFrom:     reader,
			Compressed:   string(req.InputSerialization.CompressionType),
			Expression:   cleanExpr(req.Expression),
			StreamSize:   size,
			DocumentType: req.InputSerialization.JSON.Type == JSONTypeDocument,
			Progress:     req.RequestProgress.Enabled,
		}
		if req.OutputSerialization.JSON != nil {
			options.OutputRecordDelimiter = req.OutputSerialization.JSON.RecordDelimiter
			options.OutputType = format.JSON
		}
		if req.OutputSerialization.CSV != nil {
			options.OutputFieldDelimiter = req.OutputSerialization.CSV.FieldDelimiter
			options.OutputRecordDelimiter = req.OutputSerialization.CSV.RecordDelimiter
			options.OutputType = format.CSV
		}
		// Initialize JSON input type
		s3s, err = json.New(options)
	}
	return s3s, err
}

// Execute is the function where all the blocking occurs, It writes to the HTTP
// response writer in a streaming fashion so that the client can actively use
// the results before the query is finally finished executing. The
func Execute(writer io.Writer, f format.Select) error {
	rowCh := make(chan Row)
	curBuf := bytes.NewBuffer(make([]byte, humanize.MiByte))
	curBuf.Reset()
	progressTicker := time.NewTicker(progressTime)
	continuationTimer := time.NewTimer(continuationTime)
	defer progressTicker.Stop()
	defer continuationTimer.Stop()

	go runSelectParser(f, rowCh)
	for {
		select {
		case row, ok := <-rowCh:
			if ok && row.err != nil {
				_, err := writeErrorMessage(row.err, curBuf).WriteTo(writer)
				flusher, okFlush := writer.(http.Flusher)
				if okFlush {
					flusher.Flush()
				}
				if err != nil {
					return err
				}
				curBuf.Reset()
				close(rowCh)
				return nil
			} else if ok {
				_, err := writeRecordMessage(row.record, curBuf).WriteTo(writer)
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
				_, err = writeStatMessage(statPayload, curBuf).WriteTo(writer)
				flusher, ok := writer.(http.Flusher)
				if ok {
					flusher.Flush()
				}
				if err != nil {
					return err
				}
				curBuf.Reset()
				_, err = writeEndMessage(curBuf).WriteTo(writer)
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
				_, err = writeProgressMessage(progressPayload, curBuf).WriteTo(writer)
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
			_, err := writeContinuationMessage(curBuf).WriteTo(writer)
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
