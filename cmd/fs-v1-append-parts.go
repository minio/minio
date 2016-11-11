package cmd

import (
	"errors"
	"io"
	"reflect"
	"sync"
	"time"
)

// Error sent by appendRoutine when there are holes in parts.
// For ex. let's say client uploads part-2 before part-1 in which case we
// can not append and have to wait till part-1 is uploaded. Hence we return
// this error. Currently this error is not used in the caller.
var errPartsMissing = errors.New("required parts missing")

// Error sent when appendRoutine has waited long enough and timedout.
var errAppendRoutineTimeout = errors.New("timeout")

// Timeout value for the appendRoutine go-routine.
var appendRoutineTimeout = 24 * 60 * 60 * time.Second

// Holds a map of uploadID->appendRoutine
type appendPartFS struct {
	appendRoutineMap map[string]appendRoutineInfo
	mu               sync.Mutex
}

// Input to the appendRoutine
type appendRoutineInput struct {
	meta  fsMetaV1   // list of parts that need to be appended
	errCh chan error // error sent by appendRoutine
}

// Identifies an appendRoutine.
type appendRoutineInfo struct {
	inputCh   chan appendRoutineInput
	timeoutCh chan struct{} // closed by appendRoutine when it timesout
	endCh     chan struct{} // closed after complete/abort of upload to end the appendRoutine
}

// Called after a part is uploaded so that it can be appended in the background.
func (m *appendPartFS) processPart(disk StorageAPI, bucket, object, uploadID string, meta fsMetaV1) chan error {
	m.mu.Lock()
	info, ok := m.appendRoutineMap[uploadID]
	if !ok {
		// Corresponding appendRoutine was not found, create a new one. Would happen when the first
		// part of a multipart upload is uploaded.
		inputCh := make(chan appendRoutineInput)
		timeoutCh := make(chan struct{})
		endCh := make(chan struct{})

		info = appendRoutineInfo{inputCh, timeoutCh, endCh}
		m.appendRoutineMap[uploadID] = info

		go m.appendRoutine(disk, bucket, object, uploadID, info)
	}
	m.mu.Unlock()
	errCh := make(chan error)
	go func() {
		// send input in a goroutine as send on the inputCh can block if appendRoutine
		// is busy appending a part.
		select {
		case <-info.timeoutCh:
			// This is to handle a rare race condition where we found info in m.appendRoutineMap
			// but soon after that appendRoutine timedouted out.
			errCh <- errAppendRoutineTimeout
		case info.inputCh <- appendRoutineInput{meta, errCh}:
		}
	}()
	return errCh
}

// Called after complete-multipart-upload or abort-multipart-upload so that the appendRoutine is not left dangling.
func (m *appendPartFS) endAppendRoutine(uploadID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	info, ok := m.appendRoutineMap[uploadID]
	if !ok {
		return
	}
	delete(m.appendRoutineMap, uploadID)
	close(info.endCh)
}

// The go-routine that appends the parts in the background.
func (m *appendPartFS) appendRoutine(disk StorageAPI, bucket, object, uploadID string, info appendRoutineInfo) {
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
			// Either complete-multipart-upload or abort-multipart-upload closed endCh to end the appendRoutine.
			appendFilePath := getFSAppendDataPath(uploadID)
			disk.DeleteFile(bucket, appendFilePath) // Delete the tmp append file in case abort-multipart was called.
			return
		case <-time.After(appendRoutineTimeout):
			// Timeout the goroutine to garbage collect its resources. This would happen if the client initates
			// a multipart upload and does not complete/abort it.
			m.mu.Lock()
			delete(m.appendRoutineMap, uploadID)
			m.mu.Unlock()
			close(info.timeoutCh)
			// Delete the temporary append file as well.
			appendFilePath := getFSAppendDataPath(uploadID)
			disk.DeleteFile(bucket, appendFilePath)
		}
	}
}

// Appends the "part" to the append-file inside "tmp/" that finally gets moved to the actual location
// upon complete-multipart-upload.
func appendPart(disk StorageAPI, bucket, object, uploadID string, part objectPartInfo) error {
	partPath := pathJoin(mpartMetaPrefix, bucket, object, uploadID, part.Name)
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
		n, err := disk.ReadFile(minioMetaBucket, partPath, offset, buf[:curLeft])
		if n > 0 {
			if err = disk.AppendFile(minioMetaBucket, appendFilePath, buf[:n]); err != nil {
				disk.DeleteFile(bucket, appendFilePath)
				return err
			}
		}
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			disk.DeleteFile(bucket, appendFilePath)
			return err
		}
		offset += n
		totalLeft -= n
	}
	return nil
}
