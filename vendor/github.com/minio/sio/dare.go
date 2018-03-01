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

import (
	"crypto/cipher"
	"crypto/subtle"
	"encoding/binary"
	"io"
)

type headerV10 []byte

func (h headerV10) Version() byte                { return h[0] }
func (h headerV10) Cipher() byte                 { return h[1] }
func (h headerV10) Len() int                     { return int(binary.LittleEndian.Uint16(h[2:])) + 1 }
func (h headerV10) SequenceNumber() uint32       { return binary.LittleEndian.Uint32(h[4:]) }
func (h headerV10) SetVersion()                  { h[0] = Version10 }
func (h headerV10) SetCipher(suite byte)         { h[1] = suite }
func (h headerV10) SetLen(length int)            { binary.LittleEndian.PutUint16(h[2:], uint16(length-1)) }
func (h headerV10) SetSequenceNumber(num uint32) { binary.LittleEndian.PutUint32(h[4:], num) }
func (h headerV10) SetRand(randVal []byte)       { copy(h[8:headerSize], randVal[:]) }
func (h headerV10) Nonce() []byte                { return h[4:headerSize] }
func (h headerV10) AddData() []byte              { return h[:4] }

type packageV10 []byte

func (p packageV10) Header() headerV10  { return headerV10(p[:headerSize]) }
func (p packageV10) Payload() []byte    { return p[headerSize : p.Length()-tagSize] }
func (p packageV10) Ciphertext() []byte { return p[headerSize:p.Length()] }
func (p packageV10) Length() int        { return headerSize + tagSize + p.Header().Len() }

type headerV20 []byte

func (h headerV20) Version() byte         { return h[0] }
func (h headerV20) SetVersion()           { h[0] = Version20 }
func (h headerV20) Cipher() byte          { return h[1] }
func (h headerV20) SetCipher(cipher byte) { h[1] = cipher }
func (h headerV20) Length() int           { return int(binary.LittleEndian.Uint16(h[2:4])) + 1 }
func (h headerV20) SetLength(length int)  { binary.LittleEndian.PutUint16(h[2:4], uint16(length-1)) }
func (h headerV20) IsFinal() bool         { return h[4]&0x80 == 0x80 }
func (h headerV20) Nonce() []byte         { return h[4:headerSize] }
func (h headerV20) AddData() []byte       { return h[:4] }
func (h headerV20) SetRand(randVal []byte, final bool) {
	copy(h[4:], randVal)
	if final {
		h[4] |= 0x80
	} else {
		h[4] &= 0x7F
	}
}

type packageV20 []byte

func (p packageV20) Header() headerV20  { return headerV20(p[:headerSize]) }
func (p packageV20) Payload() []byte    { return p[headerSize : headerSize+p.Header().Length()] }
func (p packageV20) Ciphertext() []byte { return p[headerSize:p.Length()] }
func (p packageV20) Length() int        { return headerSize + tagSize + p.Header().Length() }

type authEnc struct {
	CipherID byte
	SeqNum   uint32
	Cipher   cipher.AEAD
	RandVal  []byte
}

type authDec struct {
	SeqNum  uint32
	Ciphers [2]cipher.AEAD
}

type authEncV10 authEnc

func newAuthEncV10(cfg *Config) (authEncV10, error) {
	cipherID := cfg.CipherSuites[0]
	cipher, err := supportedCiphers[cipherID](cfg.Key)
	if err != nil {
		return authEncV10{}, err
	}
	var randVal [8]byte
	if _, err = io.ReadFull(cfg.Rand, randVal[:]); err != nil {
		return authEncV10{}, err
	}
	return authEncV10{
		CipherID: cipherID,
		RandVal:  randVal[:],
		Cipher:   cipher,
		SeqNum:   cfg.SequenceNumber,
	}, nil
}

func (ae *authEncV10) Seal(dst, src []byte) {
	header := headerV10(dst[:headerSize])
	header.SetVersion()
	header.SetCipher(ae.CipherID)
	header.SetLen(len(src))
	header.SetSequenceNumber(ae.SeqNum)
	header.SetRand(ae.RandVal)
	ae.Cipher.Seal(dst[headerSize:headerSize], header.Nonce(), src, header.AddData())
	ae.SeqNum++
}

type authDecV10 authDec

func newAuthDecV10(cfg *Config) (authDecV10, error) {
	var ciphers [2]cipher.AEAD
	for _, v := range cfg.CipherSuites {
		aeadCipher, err := supportedCiphers[v](cfg.Key)
		if err != nil {
			return authDecV10{}, err
		}
		ciphers[v] = aeadCipher
	}
	return authDecV10{
		SeqNum:  cfg.SequenceNumber,
		Ciphers: ciphers,
	}, nil
}

func (ad *authDecV10) Open(dst, src []byte) error {
	header := headerV10(src[:headerSize])
	if header.Version() != Version10 {
		return errUnsupportedVersion
	}
	if header.Cipher() > CHACHA20_POLY1305 {
		return errUnsupportedCipher
	}
	aeadCipher := ad.Ciphers[header.Cipher()]
	if aeadCipher == nil {
		return errUnsupportedCipher
	}
	if headerSize+header.Len()+tagSize != len(src) {
		return errInvalidPayloadSize
	}
	if header.SequenceNumber() != ad.SeqNum {
		return errPackageOutOfOrder
	}
	ciphertext := src[headerSize : headerSize+header.Len()+tagSize]
	if _, err := aeadCipher.Open(dst[:0], header.Nonce(), ciphertext, header.AddData()); err != nil {
		return errTagMismatch
	}
	ad.SeqNum++
	return nil
}

type authEncV20 struct {
	authEnc
	finalized bool
}

func newAuthEncV20(cfg *Config) (authEncV20, error) {
	cipherID := cfg.CipherSuites[0]
	cipher, err := supportedCiphers[cipherID](cfg.Key)
	if err != nil {
		return authEncV20{}, err
	}
	var randVal [12]byte
	if _, err = io.ReadFull(cfg.Rand, randVal[:]); err != nil {
		return authEncV20{}, err
	}
	return authEncV20{
		authEnc: authEnc{
			CipherID: cipherID,
			RandVal:  randVal[:],
			Cipher:   cipher,
			SeqNum:   cfg.SequenceNumber,
		},
	}, nil
}

func (ae *authEncV20) Seal(dst, src []byte)      { ae.seal(dst, src, false) }
func (ae *authEncV20) SealFinal(dst, src []byte) { ae.seal(dst, src, true) }

func (ae *authEncV20) seal(dst, src []byte, finalize bool) {
	if ae.finalized { // callers are not supposed to call Seal(Final) after a SealFinal call happened
		panic("sio: cannot seal any package after final one")
	}
	ae.finalized = finalize

	header := headerV20(dst[:headerSize])
	header.SetVersion()
	header.SetCipher(ae.CipherID)
	header.SetLength(len(src))
	header.SetRand(ae.RandVal, finalize)

	var nonce [12]byte
	copy(nonce[:], header.Nonce())
	binary.LittleEndian.PutUint32(nonce[8:], binary.LittleEndian.Uint32(nonce[8:])^ae.SeqNum)

	ae.Cipher.Seal(dst[headerSize:headerSize], nonce[:], src, header.AddData())
	ae.SeqNum++
}

type authDecV20 struct {
	authDec
	refHeader headerV20
	finalized bool
}

func newAuthDecV20(cfg *Config) (authDecV20, error) {
	var ciphers [2]cipher.AEAD
	for _, v := range cfg.CipherSuites {
		aeadCipher, err := supportedCiphers[v](cfg.Key)
		if err != nil {
			return authDecV20{}, err
		}
		ciphers[v] = aeadCipher
	}
	return authDecV20{
		authDec: authDec{
			SeqNum:  cfg.SequenceNumber,
			Ciphers: ciphers,
		},
	}, nil
}

func (ad *authDecV20) Open(dst, src []byte) error {
	if ad.finalized {
		return errUnexpectedData
	}
	if len(src) <= headerSize+tagSize {
		return errInvalidPayloadSize
	}

	header := packageV20(src).Header()
	if ad.refHeader == nil {
		ad.refHeader = make([]byte, headerSize)
		copy(ad.refHeader, header)
	}
	if header.Version() != Version20 {
		return errUnsupportedVersion
	}
	if c := header.Cipher(); c > CHACHA20_POLY1305 || ad.Ciphers[c] == nil || c != ad.refHeader.Cipher() {
		return errUnsupportedCipher
	}
	if headerSize+header.Length()+tagSize != len(src) {
		return errInvalidPayloadSize
	}
	if !header.IsFinal() && header.Length() != maxPayloadSize {
		return errInvalidPayloadSize
	}
	refNonce := ad.refHeader.Nonce()
	if header.IsFinal() {
		ad.finalized = true
		refNonce[0] |= 0x80 // set final flag
	}
	if subtle.ConstantTimeCompare(header.Nonce(), refNonce[:]) != 1 {
		return errNonceMismatch
	}

	var nonce [12]byte
	copy(nonce[:], header.Nonce())
	binary.LittleEndian.PutUint32(nonce[8:], binary.LittleEndian.Uint32(nonce[8:])^ad.SeqNum)
	cipher := ad.Ciphers[header.Cipher()]
	if _, err := cipher.Open(dst[:0], nonce[:], src[headerSize:headerSize+header.Length()+tagSize], header.AddData()); err != nil {
		return errTagMismatch
	}
	ad.SeqNum++
	return nil
}
