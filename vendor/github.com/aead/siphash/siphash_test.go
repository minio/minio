// Copyright (c) 2016 Andreas Auernhammer. All rights reserved.
// Use of this source code is governed by a license that can be
// found in the LICENSE file.

package siphash

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"testing"
)

func fromHex(s string) (b []byte) {
	b, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return
}

func TestVectors64(t *testing.T) {
	var key [16]byte
	copy(key[:], fromHex("000102030405060708090a0b0c0d0e0f"))
	h, err := New64(key[:])
	if err != nil {
		t.Fatalf("New64: %s", err)
	}

	msg := make([]byte, 64)
	for i, checksum := range vectors64 {
		msg[i] = byte(i)

		h.Write(msg[:i])
		sum := h.Sum(nil)
		if !bytes.Equal(sum, fromHex(checksum)) {
			t.Fatalf("%d (single write): got: %s want: %s", i, hex.EncodeToString(sum), checksum)
		}
		h.Reset()

		for j := 0; j < i; j++ {
			h.Write(msg[j : j+1])
		}
		sum = h.Sum(nil)
		if !bytes.Equal(sum, fromHex(checksum)) {
			t.Fatalf("%d (multi write): got: %s want: %s", i, hex.EncodeToString(sum), checksum)
		}
		h.Reset()

		tag := Sum64(msg[:i], &key)
		refTag := binary.LittleEndian.Uint64(fromHex(checksum))
		if tag != refTag {
			t.Fatalf("%d (sum): got: %x want: %x", i, tag, refTag)
		}
	}
}

func TestVectors128(t *testing.T) {
	var key [16]byte
	copy(key[:], fromHex("000102030405060708090a0b0c0d0e0f"))
	h, err := New128(key[:])
	if err != nil {
		t.Fatalf("New128: %s", err)
	}

	msg := make([]byte, 64)
	for i, checksum := range vectors128 {
		msg[i] = byte(i)

		h.Write(msg[:i])
		sum := h.Sum(nil)
		if !bytes.Equal(sum, fromHex(checksum)) {
			t.Fatalf("%d (single write): got: %s want: %s", i, hex.EncodeToString(sum), checksum)
		}
		h.Reset()

		for j := 0; j < i; j++ {
			h.Write(msg[j : j+1])
		}
		sum = h.Sum(nil)
		if !bytes.Equal(sum, fromHex(checksum)) {
			t.Fatalf("%d (multi write): got: %s want: %s", i, hex.EncodeToString(sum), checksum)
		}
		h.Reset()

		tag := Sum128(msg[:i], &key)
		if !bytes.Equal(tag[:], fromHex(checksum)) {
			t.Fatalf("%d (sum): got: %s want: %s", i, hex.EncodeToString(tag[:]), checksum)
		}
	}
}

func benchmarkWrite(b *testing.B, size int) {
	h, _ := New64(make([]byte, KeySize))
	msg := make([]byte, size)

	b.SetBytes(int64(size))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Write(msg)
	}
}

func BenchmarkWrite_8(b *testing.B)  { benchmarkWrite(b, 8) }
func BenchmarkWrite_1K(b *testing.B) { benchmarkWrite(b, 1024) }

func benchmarkSum64(b *testing.B, size int) {
	var key [16]byte
	msg := make([]byte, size)

	b.SetBytes(int64(size))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Sum64(msg, &key)
	}
}

func benchmarkSum128(b *testing.B, size int) {
	var key [16]byte
	msg := make([]byte, size)

	b.SetBytes(int64(size))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Sum128(msg, &key)
	}
}

func BenchmarkSum64_8(b *testing.B)   { benchmarkSum64(b, 8) }
func BenchmarkSum64_1K(b *testing.B)  { benchmarkSum64(b, 1024) }
func BenchmarkSum128_8(b *testing.B)  { benchmarkSum128(b, 8) }
func BenchmarkSum128_1K(b *testing.B) { benchmarkSum128(b, 1024) }

var vectors64 = []string{
	"310e0edd47db6f72", "fd67dc93c539f874", "5a4fa9d909806c0d", "2d7efbd796666785",
	"b7877127e09427cf", "8da699cd64557618", "cee3fe586e46c9cb", "37d1018bf50002ab",
	"6224939a79f5f593", "b0e4a90bdf82009e", "f3b9dd94c5bb5d7a", "a7ad6b22462fb3f4",
	"fbe50e86bc8f1e75", "903d84c02756ea14", "eef27a8e90ca23f7", "e545be4961ca29a1",
	"db9bc2577fcc2a3f", "9447be2cf5e99a69", "9cd38d96f0b3c14b", "bd6179a71dc96dbb",
	"98eea21af25cd6be", "c7673b2eb0cbf2d0", "883ea3e395675393", "c8ce5ccd8c030ca8",
	"94af49f6c650adb8", "eab8858ade92e1bc", "f315bb5bb835d817", "adcf6b0763612e2f",
	"a5c91da7acaa4dde", "716595876650a2a6", "28ef495c53a387ad", "42c341d8fa92d832",
	"ce7cf2722f512771", "e37859f94623f3a7", "381205bb1ab0e012", "ae97a10fd434e015",
	"b4a31508beff4d31", "81396229f0907902", "4d0cf49ee5d4dcca", "5c73336a76d8bf9a",
	"d0a704536ba93e0e", "925958fcd6420cad", "a915c29bc8067318", "952b79f3bc0aa6d4",
	"f21df2e41d4535f9", "87577519048f53a9", "10a56cf5dfcd9adb", "eb75095ccd986cd0",
	"51a9cb9ecba312e6", "96afadfc2ce666c7", "72fe52975a4364ee", "5a1645b276d592a1",
	"b274cb8ebf87870a", "6f9bb4203de7b381", "eaecb2a30b22a87f", "9924a43cc1315724",
	"bd838d3aafbf8db7", "0b1a2a3265d51aea", "135079a3231ce660", "932b2846e4d70666",
	"e1915f5cb1eca46c", "f325965ca16d629f", "575ff28e60381be5", "724506eb4c328a95",
}

var vectors128 = []string{
	"a3817f04ba25a8e66df67214c7550293", "da87c1d86b99af44347659119b22fc45", "8177228da4a45dc7fca38bdef60affe4", "9c70b60c5267a94e5f33b6b02985ed51",
	"f88164c12d9c8faf7d0f6e7c7bcd5579", "1368875980776f8854527a07690e9627", "14eeca338b208613485ea0308fd7a15e", "a1f1ebbed8dbc153c0b84aa61ff08239",
	"3b62a9ba6258f5610f83e264f31497b4", "264499060ad9baabc47f8b02bb6d71ed", "00110dc378146956c95447d3f3d0fbba", "0151c568386b6677a2b4dc6f81e5dc18",
	"d626b266905ef35882634df68532c125", "9869e247e9c08b10d029934fc4b952f7", "31fcefac66d7de9c7ec7485fe4494902", "5493e99933b0a8117e08ec0f97cfc3d9",
	"6ee2a4ca67b054bbfd3315bf85230577", "473d06e8738db89854c066c47ae47740", "a426e5e423bf4885294da481feaef723", "78017731cf65fab074d5208952512eb1",
	"9e25fc833f2290733e9344a5e83839eb", "568e495abe525a218a2214cd3e071d12", "4a29b54552d16b9a469c10528eff0aae", "c9d184ddd5a9f5e0cf8ce29a9abf691c",
	"2db479ae78bd50d8882a8a178a6132ad", "8ece5f042d5e447b5051b9eacb8d8f6f", "9c0b53b4b3c307e87eaee08678141f66", "abf248af69a6eae4bfd3eb2f129eeb94",
	"0664da1668574b88b935f3027358aef4", "aa4b9dc4bf337de90cd4fd3c467c6ab7", "ea5c7f471faf6bde2b1ad7d4686d2287", "2939b0183223fafc1723de4f52c43d35",
	"7c3956ca5eeafc3e363e9d556546eb68", "77c6077146f01c32b6b69d5f4ea9ffcf", "37a6986cb8847edf0925f0f1309b54de", "a705f0e69da9a8f907241a2e923c8cc8",
	"3dc47d1f29c448461e9e76ed904f6711", "0d62bf01e6fc0e1a0d3c4751c5d3692b", "8c03468bca7c669ee4fd5e084bbee7b5", "528a5bb93baf2c9c4473cce5d0d22bd9",
	"df6a301e95c95dad97ae0cc8c6913bd8", "801189902c857f39e73591285e70b6db", "e617346ac9c231bb3650ae34ccca0c5b", "27d93437efb721aa401821dcec5adf89",
	"89237d9ded9c5e78d8b1c9b166cc7342", "4a6d8091bf5e7d651189fa94a250b14c", "0e33f96055e7ae893ffc0e3dcf492902", "e61c432b720b19d18ec8d84bdc63151b",
	"f7e5aef549f782cf379055a608269b16", "438d030fd0b7a54fa837f2ad201a6403", "a590d3ee4fbf04e3247e0d27f286423f", "5fe2c1a172fe93c4b15cd37caef9f538",
	"2c97325cbd06b36eb2133dd08b3a017c", "92c814227a6bca949ff0659f002ad39e", "dce850110bd8328cfbd50841d6911d87", "67f14984c7da791248e32bb5922583da",
	"1938f2cf72d54ee97e94166fa91d2a36", "74481e9646ed49fe0f6224301604698e", "57fca5de98a9d6d8006438d0583d8a1d", "9fecde1cefdc1cbed4763674d9575359",
	"e3040c00eb28f15366ca73cbd872e740", "7697009a6a831dfecca91c5993670f7a", "5853542321f567a005d547a4f04759bd", "5150d1772f50834a503e069a973fbd7c",
}
