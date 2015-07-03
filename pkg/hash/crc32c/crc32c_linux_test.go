// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Golang project:
//    https://github.com/golang/go/blob/master/LICENSE

// Using this part of Minio codebase under the license
// Apache License Version 2.0 with modifications

package crc32c

import (
	"testing"
)

type test struct {
	castagnoli uint32
	in         string
}

var golden = []test{
	{0x0, ""},
	{0x93ad1061, "a"},
	{0x13c35ee4, "ab"},
	{0x562f9ccd, "abc"},
	{0xdaaf41f6, "abcd"},
	{0x8122a0a2, "abcde"},
	{0x496937b, "abcdef"},
	{0x5d199e2c, "abcdefg"},
	{0x86bc933d, "abcdefgh"},
	{0x9639f15f, "abcdefghi"},
	{0x584645c, "abcdefghij"},
	{0x8c13a060, "Discard medicine more than two years old."},
	{0x629077d4, "He who has a shady past knows that nice guys finish last."},
	{0xd20036a4, "I wouldn't marry him with a ten foot pole."},
	{0xf283b768, "Free! Free!/A trip/to Mars/for 900/empty jars/Burma Shave"},
	{0x9cd61a9f, "The days of the digital watch are numbered.  -Tom Stoppard"},
	{0x637702f5, "Nepal premier won't resign."},
	{0x6c595588, "For every action there is an equal and opposite government program."},
	{0x19532076, "His money is twice tainted: 'taint yours and 'taint mine."},
	{0x9b82c857, "There is no reason for any individual to have a computer in their home. -Ken Olsen, 1977"},
	{0x2b485952, "It's a tiny change to the code and not completely disgusting. - Bob Manchek"},
	{0xd3d0980c, "size:  a.out:  bad magic"},
	{0x12aad0bb, "The major problem is with sendmail.  -Mark Horton"},
	{0x83a0339b, "Give me a rock, paper and scissors and I will move the world.  CCFestoon"},
	{0x1eb28fde, "If the enemy is within range, then so are you."},
	{0xce34d559, "It's well we cannot hear the screams/That we create in others' dreams."},
	{0x71576691, "You remind me of a TV show, but that's all right: I watch it anyway."},
	{0x54bf536f, "C is as portable as Stonehedge!!"},
	{0x2313a94d, "Even if I could be Shakespeare, I think I should still choose to be Faraday. - A. Huxley"},
	{0x9d4e3629, "The fugacity of a constituent in a mixture of gases at a given temperature is proportional to its mole fraction.  Lewis-Randall Rule"},
	{0xc9991fb9, "How can you write a big system without C++?  -Paul Glick"},
}

func TestGolden(t *testing.T) {
	for _, g := range golden {
		s := Sum32([]byte(g.in))
		if s != g.castagnoli {
			t.Errorf("Castagnoli(%s) = 0x%x want 0x%x", g.in, s, g.castagnoli)
		}
	}
}

func BenchmarkCrc32KB(b *testing.B) {
	b.SetBytes(1024)
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i)
	}
	h := New()
	in := make([]byte, 0, h.Size())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Reset()
		h.Write(data)
		h.Sum(in)
	}
}
