package objectv1

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"io"
	"strconv"
	"time"
)

// Package Version
const Version = uint32(1)

// ObjectType is the type of object stored. It is either an Object or Multipart Object.
type ObjectType uint8

const (
	// Object is a full object
	Object ObjectType = iota
	// MultipartObject is a collection of Objects uploaded separately that represent a large object.
	MultipartObject
)

// Object Metadata
type ObjectMetadata struct {
	Bucket      string
	Key         string
	ErasurePart uint16
	EncodedPart uint8

	ContentType string
	Created     time.Time
	Length      uint64
	Md5         []byte
	ObjectType  ObjectType
}

func Write(target io.Writer, metadata ObjectMetadata, reader io.Reader) error {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.LittleEndian, uint32(Version))
	encoder := gob.NewEncoder(buffer)
	if err := encoder.Encode(metadata); err != nil {
		return err
	}
	reader = io.MultiReader(buffer, reader)
	_, err := io.Copy(target, reader)
	return err
}

func ReadMetadata(reader io.Reader) (metadata ObjectMetadata, err error) {
	versionBytes := make([]byte, 4)
	if err := binary.Read(reader, binary.LittleEndian, versionBytes); err != nil {
		return metadata, err
	}
	var version uint32
	version = binary.LittleEndian.Uint32(versionBytes)
	if version != 1 {
		return metadata, errors.New("Unknown Version: " + strconv.FormatUint(uint64(version), 10))
	}
	decoder := gob.NewDecoder(reader)
	err = decoder.Decode(&metadata)
	return metadata, err
}
