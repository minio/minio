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

package cmd

import (
	"io"
	"testing"
)

func testBitrotReaderWriterAlgo(t *testing.T, bitrotAlgo BitrotAlgorithm) {
	tmpDir := t.TempDir()

	volume := "testvol"
	filePath := "testfile"

	disk, err := newLocalXLStorage(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	disk.MakeVol(t.Context(), volume)

	writer := newBitrotWriter(disk, "", volume, filePath, 35, bitrotAlgo, 10)

	_, err = writer.Write([]byte("aaaaaaaaaa"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = writer.Write([]byte("aaaaaaaaaa"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = writer.Write([]byte("aaaaaaaaaa"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = writer.Write([]byte("aaaaa"))
	if err != nil {
		t.Fatal(err)
	}
	if bw, ok := writer.(io.Closer); ok {
		bw.Close()
	}

	reader := newBitrotReader(disk, nil, volume, filePath, 35, bitrotAlgo, bitrotWriterSum(writer), 10)
	b := make([]byte, 10)
	if _, err = reader.ReadAt(b, 0); err != nil {
		t.Fatal(err)
	}
	if _, err = reader.ReadAt(b, 10); err != nil {
		t.Fatal(err)
	}
	if _, err = reader.ReadAt(b, 20); err != nil {
		t.Fatal(err)
	}
	if _, err = reader.ReadAt(b[:5], 30); err != nil {
		t.Fatal(err)
	}
	if br, ok := reader.(io.Closer); ok {
		br.Close()
	}
}

func TestAllBitrotAlgorithms(t *testing.T) {
	for bitrotAlgo := range bitrotAlgorithms {
		testBitrotReaderWriterAlgo(t, bitrotAlgo)
	}
}
