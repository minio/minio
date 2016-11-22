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
var errAppendPartsTimeout = errors.New("appendParts goroutine timeout")

// Timeout value for the appendParts go-routine.
var appendPartsTimeout = 24 * 60 * 60 * time.Second // 24 hours.

// Holds a map of uploadID->appendParts go-routine
type backgroundAppend struct {
	infoMap map[string]bgAppendPartsInfo
	sync.Mutex
}

// Input to the appendParts go-routine
type bgAppendPartsInput struct {
	meta  fsMetaV1   // list of parts that need to be appended
	errCh chan error // error sent by appendParts go-routine
}

// Identifies an appendParts go-routine.
type bgAppendPartsInfo struct {
	inputCh   chan bgAppendPartsInput
	timeoutCh chan struct{} // closed by appendParts go-routine when it timesout
	endCh     chan struct{} // closed after complete/abort of upload to end the appendParts go-routine
}

// Called after a part is uploaded so that it can be appended in the background.
func (b *backgroundAppend) append(disk StorageAPI, bucket, object, uploadID string, meta fsMetaV1) {
	b.Lock()
	info, ok := b.infoMap[uploadID]
	if !ok {
		// Corresponding appendParts go-routine was not found, create a new one. Would happen when the first
		// part of a multipart upload is uploaded.
		inputCh := make(chan bgAppendPartsInput)
		timeoutCh := make(chan struct{})
		endCh := make(chan struct{})

		info = bgAppendPartsInfo{inputCh, timeoutCh, endCh}
		b.infoMap[uploadID] = info

		go b.appendParts(disk, bucket, object, uploadID, info)
	}
	b.Unlock()
	go func() {
		errCh := make(chan error)
		// send input in a goroutine as send on the inputCh can block if appendParts go-routine
		// is busy appending a part.
		select {
		case <-info.timeoutCh:
			// This is to handle a rare race condition where we found info in b.infoMap
			// but soon after that appendParts go-routine timed out.
		case info.inputCh <- bgAppendPartsInput{meta, errCh}:
			// Receive the error so that the appendParts go-routine does not block on send.
			// But the error received is ignored as fs.PutObjectPart() would have already
			// returned success to the client.
			<-errCh
		}
	}()
}

// Called on complete-multipart-upload. Returns nil if the required parts have been appended.
func (b *backgroundAppend) complete(disk StorageAPI, bucket, object, uploadID string, meta fsMetaV1) error {
	b.Lock()
	info, ok := b.infoMap[uploadID]
	b.Unlock()
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

	b.remove(uploadID)

	return err
}

// Called after complete-multipart-upload or abort-multipart-upload so that the appendParts go-routine is not left dangling.
func (b *backgroundAppend) remove(uploadID string) {
	b.Lock()
	defer b.Unlock()
	info, ok := b.infoMap[uploadID]
	if !ok {
		return
	}
	delete(b.infoMap, uploadID)
	close(info.endCh)
}

// This is run as a go-routine that appends the parts in the background.
func (b *backgroundAppend) appendParts(disk StorageAPI, bucket, object, uploadID string, info bgAppendPartsInfo) {
	// Holds the list of parts that is already appended to the "append" file.
	appendMeta := fsMetaV1{}
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
				if err := appendPart(disk, bucket, object, uploadID, part); err != nil {
					input.errCh <- err
					break
				}
				appendMeta.AddObjectPart(part.Number, part.Name, part.ETag, part.Size)
			}
		case <-info.endCh:
			// Either complete-multipart-upload or abort-multipart-upload closed endCh to end the appendParts go-routine.
			appendFilePath := getFSAppendDataPath(uploadID)
			disk.DeleteFile(bucket, appendFilePath)
			return
		case <-time.After(appendPartsTimeout):
			// Timeout the goroutine to garbage collect its resources. This would happen if the client initiates
			// a multipart upload and does not complete/abort it.
			b.Lock()
			delete(b.infoMap, uploadID)
			b.Unlock()
			// Delete the temporary append file as well.
			appendFilePath := getFSAppendDataPath(uploadID)
			disk.DeleteFile(bucket, appendFilePath)

			close(info.timeoutCh)
		}
	}
}

// Appends the "part" to the append-file inside "tmp/" that finally gets moved to the actual location
// upon complete-multipart-upload.
func appendPart(disk StorageAPI, bucket, object, uploadID string, part objectPartInfo) error {
	partPath := pathJoin(bucket, object, uploadID, part.Name)
	appendFilePath := getFSAppendDataPath(uploadID)

	offset := int64(0)
	totalLeft := part.Size
	buf := make([]byte, readSizeV1)
	for totalLeft > 0 {
		curLeft := int64(readSizeV1)
		if totalLeft < readSizeV1 {
			curLeft = totalLeft
		}
		var n int64
		n, err := disk.ReadFile(minioMetaMultipartBucket, partPath, offset, buf[:curLeft])
		if err != nil {
			// Check for EOF/ErrUnexpectedEOF not needed as it should never happen as we know
			// the exact size of the file and hence know the size of buf[]
			// EOF/ErrUnexpectedEOF indicates that the length of file was shorter than part.Size and
			// hence considered as an error condition.
			disk.DeleteFile(bucket, appendFilePath)
			return err
		}
		if err = disk.AppendFile(minioMetaBucket, appendFilePath, buf[:n]); err != nil {
			disk.DeleteFile(bucket, appendFilePath)
			return err
		}
		offset += n
		totalLeft -= n
	}
	return nil
}
