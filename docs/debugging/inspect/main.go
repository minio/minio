// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"strings"

	"github.com/secure-io/sio-go"
)

var (
	key = flag.String("key", "", "decryption string")
	//js = flag.Bool("json", false, "expect json input")
)

func main() {
	flag.Parse()
	args := flag.Args()
	switch len(flag.Args()) {
	case 0:
		// Read from stdin, write to stdout.
		decrypt(*key, os.Stdin, os.Stdout)
		return
	case 1:
		r, err := os.Open(args[0])
		fatalErr(err)
		defer r.Close()
		dstName := strings.TrimSuffix(args[0], ".enc") + ".zip"
		w, err := os.Create(dstName)
		fatalErr(err)
		defer w.Close()
		if len(*key) == 0 {
			reader := bufio.NewReader(os.Stdin)
			fmt.Print("Enter Decryption Key: ")

			text, _ := reader.ReadString('\n')
			// convert CRLF to LF
			*key = strings.Replace(text, "\n", "", -1)
		}
		decrypt(*key, r, w)
		fmt.Println("Output decrypted to", dstName)
		return
	default:
		fatalIf(true, "Only 1 file can be decrypted")
		os.Exit(1)
	}
}

func decrypt(keyHex string, r io.Reader, w io.Writer) {
	keyHex = strings.TrimSpace(keyHex)
	fatalIf(len(keyHex) != 72, "Unexpected key length: %d, want 72", len(keyHex))
	id, err := hex.DecodeString(keyHex[:8])
	fatalErr(err)
	key, err := hex.DecodeString(keyHex[8:])
	fatalErr(err)

	// Verify that CRC is ok.
	want := binary.LittleEndian.Uint32(id)
	got := crc32.ChecksumIEEE(key)
	fatalIf(want != got, "Invalid key checksum, want %x, got %x", want, got)

	stream, err := sio.AES_256_GCM.Stream(key)
	fatalErr(err)

	// Zero nonce, we only use each key once, and 32 bytes is plenty.
	nonce := make([]byte, stream.NonceSize())
	encr := stream.DecryptReader(r, nonce, nil)
	_, err = io.Copy(w, encr)
	fatalErr(err)
}

func fatalErr(err error) {
	if err == nil {
		return
	}
	log.Fatalln(err)
}

func fatalIf(b bool, msg string, v ...interface{}) {
	if !b {
		return
	}
	log.Fatalf(msg, v...)
}
