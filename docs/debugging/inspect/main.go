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
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/secure-io/sio-go"
)

var (
	key = flag.String("key", "", "decryption string")
	js  = flag.Bool("json", false, "expect json input from stdin")
)

func main() {
	flag.Parse()

	if *js {
		// Match struct in https://github.com/minio/mc/blob/b3ce21fb72a914f50522358668e6464eb97de8d1/cmd/admin-inspect.go#L135
		input := struct {
			File string `json:"file"`
			Key  string `json:"key"`
		}{}
		got, err := ioutil.ReadAll(os.Stdin)
		if err != nil {
			fatalErr(err)
		}
		fatalErr(json.Unmarshal(got, &input))
		r, err := os.Open(input.File)
		fatalErr(err)
		defer r.Close()
		dstName := strings.TrimSuffix(input.File, ".enc") + ".zip"
		w, err := os.Create(dstName)
		fatalErr(err)
		defer w.Close()
		decrypt(input.Key, r, w)
		fmt.Println("Output decrypted to", dstName)
		return
	}
	args := flag.Args()
	switch len(flag.Args()) {
	case 0:
		// Read from stdin, write to stdout.
		if *key == "" {
			flag.Usage()
			fatalErr(errors.New("no key supplied"))
		}
		decrypt(*key, os.Stdin, os.Stdout)
		return
	case 1:
		r, err := os.Open(args[0])
		fatalErr(err)
		defer r.Close()
		if len(*key) == 0 {
			reader := bufio.NewReader(os.Stdin)
			fmt.Print("Enter Decryption Key: ")

			text, _ := reader.ReadString('\n')
			// convert CRLF to LF
			*key = strings.Replace(text, "\n", "", -1)
		}
		*key = strings.TrimSpace(*key)
		fatalIf(len(*key) != 72, "Unexpected key length: %d, want 72", len(*key))

		dstName := strings.TrimSuffix(args[0], ".enc") + ".zip"
		w, err := os.Create(dstName)
		fatalErr(err)
		defer w.Close()

		decrypt(*key, r, w)
		fmt.Println("Output decrypted to", dstName)
		return
	default:
		flag.Usage()
		fatalIf(true, "Only 1 file can be decrypted")
		os.Exit(1)
	}
}

func decrypt(keyHex string, r io.Reader, w io.Writer) {
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
