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
	"testing"
)

func Test_getID(t *testing.T) {
	mids := &messageIds{index: make(map[uint16]tokenCompletor)}

	i1 := mids.getID(&DummyToken{})

	if i1 != 1 {
		t.Fatalf("i1 was wrong: %v", i1)
	}

	i2 := mids.getID(&DummyToken{})

	if i2 != 2 {
		t.Fatalf("i2 was wrong: %v", i2)
	}

	for i := uint16(3); i < 100; i++ {
		id := mids.getID(&DummyToken{})
		if id != i {
			t.Fatalf("id was wrong expected %v got %v", i, id)
		}
	}
}

func Test_freeID(t *testing.T) {
	mids := &messageIds{index: make(map[uint16]tokenCompletor)}

	i1 := mids.getID(&DummyToken{})
	mids.freeID(i1)

	if i1 != 1 {
		t.Fatalf("i1 was wrong: %v", i1)
	}

	i2 := mids.getID(&DummyToken{})
	fmt.Printf("i2: %v\n", i2)
}
