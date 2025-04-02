// Copyright (c) 2015-2023 MinIO, Inc.
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
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"github.com/minio/madmin-go/v3/estream"
)

type keepFileErr struct {
	error
}

func extractInspectV2(pks [][]byte, r io.Reader, extractDir string) error {
	sr, err := estream.NewReader(r)
	if err != nil {
		return err
	}
	for _, pk := range pks {
		privKey, err := bytesToPrivateKey(pk)
		if err != nil {
			return fmt.Errorf("decoding key returned: %w", err)
		}
		sr.SetPrivateKey(privKey)
	}
	sr.ReturnNonDecryptable(true)

	// Debug corrupted streams.
	if false {
		sr.SkipEncrypted(true)
		return sr.DebugStream(os.Stdout)
	}
	extracted := false
	for {
		stream, err := sr.NextStream()
		if err != nil {
			if err == io.EOF {
				if extracted {
					return nil
				}
				return errors.New("no data found on stream")
			}
			if errors.Is(err, estream.ErrNoKey) {
				fmt.Println("Skipping", stream.Name, "no private key")
				if err := stream.Skip(); err != nil {
					return fmt.Errorf("stream skip: %w", err)
				}
				continue
			}
			return fmt.Errorf("next stream: %w", err)
		}
		if strings.Contains(stream.Name, "..") || !utf8.ValidString(stream.Name) {
			return fmt.Errorf("invalid stream name: %q", stream.Name)
		}

		dst := filepath.Join(extractDir, stream.Name)
		os.Mkdir(extractDir, 0o755)
		w, err := os.Create(dst)
		if err != nil {
			return fmt.Errorf("creating output file: %w", err)
		}
		_, err = io.Copy(w, stream)
		if err != nil {
			return fmt.Errorf("reading inspect stream: %w", err)
		}
		fmt.Printf("Extracted: %s\n", dst)
		extracted = true
	}
}
