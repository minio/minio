// Copyright (C) 2017 Minio Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sio

import "encoding/binary"

type headerV10 []byte

func header(b []byte) headerV10 { return headerV10(b[:headerSize]) }

func (h headerV10) Version() byte { return h[0] }

func (h headerV10) Cipher() byte { return h[1] }

func (h headerV10) Len() int { return int(binary.LittleEndian.Uint16(h[2:])) + 1 }

func (h headerV10) SequenceNumber() uint32 { return binary.LittleEndian.Uint32(h[4:]) }

func (h headerV10) SetVersion(version byte) { h[0] = version }

func (h headerV10) SetCipher(suite byte) { h[1] = suite }

func (h headerV10) SetLen(length int) { binary.LittleEndian.PutUint16(h[2:], uint16(length-1)) }

func (h headerV10) SetSequenceNumber(num uint32) { binary.LittleEndian.PutUint32(h[4:], num) }

func (h headerV10) SetNonce(nonce [8]byte) { copy(h[8:], nonce[:]) }
