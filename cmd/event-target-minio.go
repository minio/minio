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

package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"strings"
	"sync/atomic"

	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/event"
	"github.com/minio/minio/pkg/hash"
)

// Minio constants
const (
	MinioBucket = "bucket"
	MinioEvents = "events"

	EnvMinioEnable = "MINIO_NOTIFY_MINIO_ENABLE"
	EnvMinioBucket = "MINIO_NOTIFY_MINIO_BUCKET"
	EnvMinioEvents = "MINIO_NOTIFY_MINIO_EVENTS"
)

// HelpMinio - help for minio target
var HelpMinio = config.HelpKVS{
	config.HelpKV{
		Key:         MinioBucket,
		Description: "bucket on MinIO where logs are getting stored",
		Type:        "string",
	},
	config.HelpKV{
		Key:         MinioEvents,
		Description: "number of events per log",
		Type:        "string",
		Optional:    true,
	},
}

// DefaultMinioKVS - default keys for minio target
var DefaultMinioKVS = config.KVS{
	config.KV{
		Key:   config.Enable,
		Value: config.EnableOff,
	},
	config.KV{
		Key:   MinioBucket,
		Value: "",
	},
	config.KV{
		Key:   MinioEvents,
		Value: "10000",
	},
}

// MinioArgs - Minio target arguments.
type MinioArgs struct {
	Enable    bool
	Bucket    string
	Store     ObjectLayer
	NumEvents int64
	Writer    *io.PipeWriter
	Reader    *io.PipeReader
}

// Validate MinioArgs fields
func (w MinioArgs) Validate() error {
	if w.Bucket == "" {
		return errors.New("bucket cannot be empty")
	}
	if w.Store == nil {
		return errors.New("store cannot be uninitialized")
	}
	return nil
}

// MinioTarget - Minio target.
type MinioTarget struct {
	id          event.TargetID
	args        MinioArgs
	doneCh      <-chan struct{}
	eofCh       chan struct{}
	eventsCount int64
}

// ID - returns target ID.
func (target MinioTarget) ID() event.TargetID {
	return target.id
}

// Save - saves the events to the store if queuestore is configured,
// which will be replayed when the wenhook connection is active.
func (target *MinioTarget) Save(eventData event.Event) error {
	return target.send(eventData)
}

type eofReader struct {
	io.Reader
	eofCh <-chan struct{}
}

func (e *eofReader) Read(b []byte) (int, error) {
	select {
	case <-e.eofCh:
		return 0, io.EOF
	default:
		return e.Reader.Read(b)
	}
}

func (target *MinioTarget) startMinioTarget() {
	for {
		select {
		default:
			uuid := strings.ReplaceAll(mustGetUUID(), "-", SlashSeparator) + ".json"

			hashReader, err := hash.NewReader(&eofReader{
				Reader: target.args.Reader,
				eofCh:  target.eofCh,
			}, -1, "", "", -1, false)
			if err != nil {
				logger.LogIf(context.Background(), err)
				continue
			}

			_, err = target.args.Store.PutObject(context.Background(), target.args.Bucket, uuid,
				NewPutObjReader(hashReader, nil, nil), ObjectOptions{
					UserDefined: map[string]string{
						"content-type": "application/json",
					},
				})
			if err != nil {
				logger.LogIf(context.Background(), err)
				continue
			}
		case <-target.doneCh:
			return
		}
	}
}

// send - sends an event to the minio.
func (target *MinioTarget) send(eventData event.Event) error {
	objectName, err := url.QueryUnescape(eventData.S3.Object.Key)
	if err != nil {
		return err
	}

	data, err := json.Marshal(event.Log{
		EventName: eventData.EventName,
		Key:       objectName,
		Records:   []event.Event{eventData},
	})
	if err != nil {
		return err
	}

	if c := atomic.LoadInt64(&target.eventsCount); c > target.args.NumEvents {
		atomic.StoreInt64(&target.eventsCount, 0)
	}

	if c := atomic.AddInt64(&target.eventsCount, 1); c > target.args.NumEvents {
		target.eofCh <- struct{}{} // writes a new object after N number of events.
	}

	_, err = target.args.Writer.Write(data)
	return err
}

// IsActive - dummy method returns always true.
func (target *MinioTarget) IsActive() (bool, error) {
	return true, nil
}

// Send - reads an event from store and sends it to minio.
func (target *MinioTarget) Send(eventKey string) error {
	return nil
}

// Close - does nothing and available for interface compatibility.
func (target *MinioTarget) Close() error {
	return nil
}

// NewMinioTarget - creates new Minio target.
func NewMinioTarget(id string, args MinioArgs, doneCh <-chan struct{}) (*MinioTarget, error) {
	target := &MinioTarget{
		id:          event.TargetID{ID: id, Name: "minio"},
		args:        args,
		doneCh:      doneCh,
		eofCh:       make(chan struct{}),
		eventsCount: 0,
	}

	if args.Enable {
		if err := args.Validate(); err != nil {
			return nil, err
		}
		go target.startMinioTarget()
	}

	return target, nil
}
