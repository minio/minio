/*
 * Copyright (c) 2013 IBM Corp.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 *    Seth Hoenig
 *    Allan Stockdill-Mander
 *    Mike Robertson
 */

package mqtt

import (
	"fmt"
	"sync"
	"time"
)

// MId is 16 bit message id as specified by the MQTT spec.
// In general, these values should not be depended upon by
// the client application.
type MId uint16

type messageIds struct {
	sync.RWMutex
	index map[uint16]tokenCompletor
}

const (
	midMin uint16 = 1
	midMax uint16 = 65535
)

func (mids *messageIds) cleanUp() {
	mids.Lock()
	for _, token := range mids.index {
		switch t := token.(type) {
		case *PublishToken:
			t.err = fmt.Errorf("Connection lost before Publish completed")
		case *SubscribeToken:
			t.err = fmt.Errorf("Connection lost before Subscribe completed")
		case *UnsubscribeToken:
			t.err = fmt.Errorf("Connection lost before Unsubscribe completed")
		case nil:
			continue
		}
		token.flowComplete()
	}
	mids.index = make(map[uint16]tokenCompletor)
	mids.Unlock()
	DEBUG.Println(MID, "cleaned up")
}

func (mids *messageIds) freeID(id uint16) {
	mids.Lock()
	delete(mids.index, id)
	mids.Unlock()
}

func (mids *messageIds) claimID(token tokenCompletor, id uint16) {
	mids.Lock()
	defer mids.Unlock()
	if _, ok := mids.index[id]; !ok {
		mids.index[id] = token
	} else {
		old := mids.index[id]
		old.flowComplete()
		mids.index[id] = token
	}
}

func (mids *messageIds) getID(t tokenCompletor) uint16 {
	mids.Lock()
	defer mids.Unlock()
	for i := midMin; i < midMax; i++ {
		if _, ok := mids.index[i]; !ok {
			mids.index[i] = t
			return i
		}
	}
	return 0
}

func (mids *messageIds) getToken(id uint16) tokenCompletor {
	mids.RLock()
	defer mids.RUnlock()
	if token, ok := mids.index[id]; ok {
		return token
	}
	return &DummyToken{id: id}
}

type DummyToken struct {
	id uint16
}

func (d *DummyToken) Wait() bool {
	return true
}

func (d *DummyToken) WaitTimeout(t time.Duration) bool {
	return true
}

func (d *DummyToken) flowComplete() {
	ERROR.Printf("A lookup for token %d returned nil\n", d.id)
}

func (d *DummyToken) Error() error {
	return nil
}

func (d *DummyToken) setError(e error) {}
