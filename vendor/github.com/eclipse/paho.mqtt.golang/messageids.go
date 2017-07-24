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
)

// MId is 16 bit message id as specified by the MQTT spec.
// In general, these values should not be depended upon by
// the client application.
type MId uint16

type messageIds struct {
	sync.RWMutex
	index map[uint16]Token
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
		}
		token.flowComplete()
	}
	mids.index = make(map[uint16]Token)
	mids.Unlock()
}

func (mids *messageIds) freeID(id uint16) {
	mids.Lock()
	delete(mids.index, id)
	mids.Unlock()
}

func (mids *messageIds) getID(t Token) uint16 {
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

func (mids *messageIds) getToken(id uint16) Token {
	mids.RLock()
	defer mids.RUnlock()
	if token, ok := mids.index[id]; ok {
		return token
	}
	return nil
}
