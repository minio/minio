package zipindex

import (
	"bytes"
	"errors"
	"io"
)

// SET GO111_MODULE=off&&go-fuzz-build -o=fuzz-build.zip&&go-fuzz -minimize=5s -timeout=60 -bin=fuzz-build.zip -workdir=fuzz

func Fuzz(b []byte) int {
	exitOnErr := func(err error) {
		if err != nil {
			panic(err)
		}
	}
	sz := 1 << 10
	if sz > len(b) {
		sz = len(b)
	}
	var files Files
	var err error
	for {
		files, err = ReadDir(b[len(b)-sz:], int64(len(b)))
		if err == nil {
			break
		}
		var terr ErrNeedMoreData
		if errors.As(err, &terr) {
			if terr.FromEnd > int64(len(b)) {
				return 0
			}
			sz = int(terr.FromEnd)
		} else {
			// Unable to parse...
			return 0
		}
	}
	// Serialize files to binary.
	serialized, err := files.Serialize()
	exitOnErr(err)

	// Deserialize the content.
	files, err = DeserializeFiles(serialized)
	exitOnErr(err)

	if len(files) == 0 {
		return 0
	}
	file := files[0]

	// Create a reader with entire zip file...
	rs := bytes.NewReader(b)
	// Seek to the file offset.
	_, err = rs.Seek(file.Offset, io.SeekStart)
	if err != nil {
		return 0
	}

	// Provide the forwarded reader..
	rc, err := file.Open(rs)
	if err != nil {
		return 1
	}
	defer rc.Close()

	// Read the zip file content.
	io.ReadAll(rc)
	exitOnErr(err)

	return 1
}
