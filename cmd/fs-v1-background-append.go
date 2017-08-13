/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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
	"errors"
	"io"
	"os"
	"reflect"
	"sync"
	"time"
)

// Error sent by appendParts go-routine when there are holes in parts.
// For ex. let's say client uploads part-2 before part-1 in which case we
// can not append and have to wait till part-1 is uploaded. Hence we return
// this error. Currently this error is not used in the caller.
var errPartsMissing = errors.New("required parts missing")

// Error sent when appendParts go-routine has waited long enough and timedout.
var errAppendPartsTimeout = errors.New("appendParts go-routine timeout")

// Timeout value for the appendParts go-routine.
var appendPartsTimeout = 24 * 60 * 60 * time.Second // 24 Hours.

// Holds a map of uploadID->appendParts go-routine
type backgroundAppend struct {
	sync.Mutex
	infoMap    map[string]bgAppendPartsInfo
	appendFile io.WriteCloser
}

// Input to the appendParts go-routine
type bgAppendPartsInput struct {
	meta  fsMetaV1   // list of parts that need to be appended
	errCh chan error // error sent by appendParts go-routine
}

// Identifies an appendParts go-routine.
type bgAppendPartsInfo struct {
	inputCh    chan bgAppendPartsInput
	timeoutCh  chan struct{} // closed by appendParts go-routine when it timesout
	abortCh    chan struct{} // closed after abort of upload to end the appendParts go-routine
	completeCh chan struct{} // closed after complete of upload to end the appendParts go-routine
}

// Called after a part is uploaded so that it can be appended in the background.
func (fs fsObjects) append(bucket, object, uploadID string, meta fsMetaV1) chan error {
	fs.bgAppend.Lock()
	info, ok := fs.bgAppend.infoMap[uploadID]
	if !ok {
		// Corresponding appendParts go-routine was not found, create a new one. Would happen when the first
		// part of a multipart upload is uploaded.
		inputCh := make(chan bgAppendPartsInput)
		timeoutCh := make(chan struct{})
		abortCh := make(chan struct{})
		completeCh := make(chan struct{})

		info = bgAppendPartsInfo{inputCh, timeoutCh, abortCh, completeCh}
		fs.bgAppend.infoMap[uploadID] = info

		go fs.appendParts(bucket, object, uploadID, info)
	}
	fs.bgAppend.Unlock()

	errCh := make(chan error)
	go func() {
		// send input in a goroutine as send on the inputCh can block if appendParts go-routine
		// is busy appending a part.
		select {
		case <-info.timeoutCh:
			// This is to handle a rare race condition where we found info in b.infoMap
			// but soon after that appendParts go-routine timed out.
			errCh <- errAppendPartsTimeout
		case info.inputCh <- bgAppendPartsInput{meta, errCh}:
		}
	}()

	return errCh
}

// Called on complete-multipart-upload. Returns nil if the required parts have been appended.
func (fs *fsObjects) complete(bucket, object, uploadID string, meta fsMetaV1) error {
	fs.bgAppend.Lock()
	defer fs.bgAppend.Unlock()

	info, ok := fs.bgAppend.infoMap[uploadID]
	delete(fs.bgAppend.infoMap, uploadID)
	if !ok {
		return errPartsMissing
	}

	errCh := make(chan error)

	select {
	case <-info.timeoutCh:
		// This is to handle a rare race condition where we found info in b.infoMap
		// but soon after that appendParts go-routine timedouted out.
		return errAppendPartsTimeout
	case info.inputCh <- bgAppendPartsInput{meta, errCh}:
	}

	err := <-errCh

	close(info.completeCh)

	return err
}

// Called after complete-multipart-upload or abort-multipart-upload so that the appendParts go-routine is not left dangling.
func (fs fsObjects) abort(uploadID string) {
	fs.bgAppend.Lock()
	defer fs.bgAppend.Unlock()

	info, ok := fs.bgAppend.infoMap[uploadID]
	if !ok {
		return
	}

	delete(fs.bgAppend.infoMap, uploadID)

	info.abortCh <- struct{}{}
}

// This is run as a go-routine that appends the parts in the background.
func (fs fsObjects) appendParts(bucket, object, uploadID string, info bgAppendPartsInfo) {
	appendPath := pathJoin(fs.fsPath, minioMetaTmpBucket, fs.fsUUID, uploadID)
	// Holds the list of parts that is already appended to the "append" file.
	appendMeta := fsMetaV1{}

	// Allocate staging read buffer.
	buf := make([]byte, readSizeV1)
	for {
		select {
		case input := <-info.inputCh:
			// We receive on this channel when new part gets uploaded or when complete-multipart sends
			// a value on this channel to confirm if all the required parts are appended.
			meta := input.meta

			for {
				// Append should be done such a way that if part-3 and part-2 is uploaded before part-1, we
				// wait till part-1 is uploaded after which we append part-2 and part-3 as well in this for-loop.
				part, appendNeeded := partToAppend(meta, appendMeta)
				if !appendNeeded {
					if reflect.DeepEqual(meta.Parts, appendMeta.Parts) {
						// Sending nil is useful so that the complete-multipart-upload knows that
						// all the required parts have been appended.
						input.errCh <- nil
					} else {
						// Sending error is useful so that complete-multipart-upload can fall-back to
						// its own append process.
						input.errCh <- errPartsMissing
					}
					break
				}

				if err := fs.appendPart(bucket, object, uploadID, part, buf); err != nil {
					fsRemoveFile(appendPath)
					appendMeta.Parts = nil
					input.errCh <- err
					break
				}

				appendMeta.AddObjectPart(part.Number, part.Name, part.ETag, part.Size)
			}
		case <-info.abortCh:
			// abort-multipart-upload closed abortCh to end the appendParts go-routine.
			fsRemoveFile(appendPath)

			// So that any racing PutObjectPart does not leave a dangling go-routine.
			close(info.timeoutCh)

			return
		case <-info.completeCh:
			// complete-multipart-upload closed completeCh to end the appendParts go-routine.
			close(info.timeoutCh) // So that any racing PutObjectPart does not leave a dangling go-routine.
			return
		case <-time.After(appendPartsTimeout):
			// Timeout the goroutine to garbage collect its resources. This would happen if the client initiates
			// a multipart upload and does not complete/abort it.
			fs.bgAppend.Lock()
			delete(fs.bgAppend.infoMap, uploadID)
			fs.bgAppend.Unlock()

			// Delete the temporary append file as well.
			fsRemoveFile(appendPath)

			close(info.timeoutCh)
			return
		}
	}
}

// Appends the "part" to the append-file inside "tmp/" that finally gets moved to the actual location
// upon complete-multipart-upload.
func (fs fsObjects) appendPart(bucket, object, uploadID string, part objectPartInfo, buf []byte) error {
	partPath := pathJoin(fs.fsPath, minioMetaMultipartBucket, bucket, object, uploadID, part.Name)

	var offset int64
	// Read each file part to start writing to the temporary concatenated object.
	file, size, err := fsOpenFile(partPath, offset)
	if err != nil {
		if err == errFileNotFound {
			return errPartsMissing
		}
		return err
	}
	defer file.Close()

	tmpObjPath := pathJoin(fs.fsPath, minioMetaTmpBucket, fs.fsUUID, uploadID)
	// No need to hold a lock, this is a unique file and will be only written
	// to one one process per uploadID per minio process.
	wfile, err := os.OpenFile((tmpObjPath), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		return err
	}
	defer wfile.Close()

	// Fallocate more space as we concatenate.
	if err = fsFAllocate(int(wfile.Fd()), 0, size); err != nil {
		return err
	}

	_, err = io.CopyBuffer(wfile, file, buf)
	return err
}
