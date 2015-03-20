/*
 * Minimalist Object Storage, (C) 2014 Minio, Inc.
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

package split

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
)

// Message - message structure for results from the Stream goroutine
type Message struct {
	Data []byte
	Err  error
}

// Stream reads from io.Reader, splits the data into chunks, and sends
// each chunk to the channel. This method runs until an EOF or error occurs. If
// an error occurs, the method sends the error over the channel and returns.
// Before returning, the channel is always closed.
//
// The user should run this as a gorountine and retrieve the data over the
// channel.
//
//  channel := make(chan Message)
//  go Stream(reader, chunkSize, channel)
//  for chunk := range channel {
//    log.Println(chunk.Data)
//  }
func Stream(reader io.Reader, chunkSize uint64) <-chan Message {
	ch := make(chan Message)
	go splitStreamGoRoutine(reader, chunkSize, ch)
	return ch
}

func splitStreamGoRoutine(reader io.Reader, chunkSize uint64, ch chan Message) {
	// we read until EOF or another error
	var readError error

	// run this until an EOF or error occurs
	for readError == nil {
		// keep track of how much data has been read
		var totalRead uint64
		// Create a buffer to write the current chunk into
		var bytesBuffer bytes.Buffer
		bytesWriter := bufio.NewWriter(&bytesBuffer)
		// read a full chunk
		for totalRead < chunkSize && readError == nil {
			var currentRead int
			// if we didn't read a full chunk, we should attempt to read again.
			// We create a byte array representing how much space is left
			// unwritten in the given chunk
			chunk := make([]byte, chunkSize-totalRead)
			currentRead, readError = reader.Read(chunk)
			// keep track of how much we have read in total
			totalRead = totalRead + uint64(currentRead)
			// prune the array to only what has been read, write to chunk buffer
			chunk = chunk[0:currentRead]
			bytesWriter.Write(chunk)
		}
		// flush stream to underlying byte buffer
		bytesWriter.Flush()
		// if we have data available, send it over the channel
		if bytesBuffer.Len() != 0 {
			ch <- Message{bytesBuffer.Bytes(), nil}
		}
	}
	// if we have an error other than an EOF, send it over the channel
	if readError != io.EOF {
		ch <- Message{nil, readError}
	}
	// close the channel, signaling the channel reader that the stream is complete
	close(ch)
}

// JoinFiles reads from a given directory, joins data in chunks with prefix and sends
// an io.Reader.
//
//  var err error
//  for err == nil {
//     buf := make([]byte, 1024*1024)
//     reader := JoinFiles("mydirectory", "mypreferred-prefix")
//     _, err = reader.Read(buf)
//     fmt.Println(buf)
// }
//
func JoinFiles(dirname string, inputPrefix string) (io.Reader, error) {
	reader, writer := io.Pipe()
	fileInfos, readError := ioutil.ReadDir(dirname)
	if readError != nil {
		writer.CloseWithError(readError)
		return nil, readError
	}

	var newfileInfos []os.FileInfo
	for _, fi := range fileInfos {
		if strings.Contains(fi.Name(), inputPrefix) == true {
			newfileInfos = append(newfileInfos, fi)
		}
	}

	if len(newfileInfos) == 0 {
		nofilesError := errors.New("no files found for given prefix " + inputPrefix)
		writer.CloseWithError(nofilesError)
		return nil, nofilesError
	}

	go joinFilesGoRoutine(newfileInfos, writer)
	return reader, nil
}

func joinFilesGoRoutine(fileInfos []os.FileInfo, writer *io.PipeWriter) {
	for _, fileInfo := range fileInfos {
		file, err := os.Open(fileInfo.Name())
		defer file.Close()
		for err != nil {
			writer.CloseWithError(err)
			return
		}
		_, err = io.Copy(writer, file)
		if err != nil {
			writer.CloseWithError(err)
			return
		}
	}
	writer.Close()
}

// FileWithPrefix - Takes a file and splits it into chunks with size chunkSize. The output
// filename is given with outputPrefix.
func FileWithPrefix(filename string, chunkSize uint64, outputPrefix string) error {
	// open file
	file, err := os.Open(filename)
	defer file.Close()
	if err != nil {
		return err
	}

	if outputPrefix == "" {
		return errors.New("Invalid argument outputPrefix cannot be empty string")
	}

	// start stream splitting goroutine
	ch := Stream(file, chunkSize)

	// used to write each chunk out as a separate file. {{outputPrefix}}.{{i}}
	i := 0

	// write each chunk out to a separate file
	for chunk := range ch {
		if chunk.Err != nil {
			return chunk.Err
		}
		err := ioutil.WriteFile(outputPrefix+"."+strconv.Itoa(i), chunk.Data, 0600)
		if err != nil {
			return err
		}
		i = i + 1
	}
	return nil
}
