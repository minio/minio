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
	"fmt"
	"time"

	"github.com/tinylib/msgp/msgp"
)

// unmarshalV unmarshals with a specific header version.
func (x *xlMetaV2VersionHeader) unmarshalV(v uint8, bts []byte) (o []byte, err error) {
	switch v {
	case 1:
		return x.unmarshalV1(bts)
	case 2:
		x2 := xlMetaV2VersionHeaderV2{xlMetaV2VersionHeader: x}
		return x2.UnmarshalMsg(bts)
	case xlHeaderVersion:
		return x.UnmarshalMsg(bts)
	}
	return bts, fmt.Errorf("unknown xlHeaderVersion: %d", v)
}

// unmarshalV1 decodes version 1, never released.
func (x *xlMetaV2VersionHeader) unmarshalV1(bts []byte) (o []byte, err error) {
	var zb0001 uint32
	zb0001, bts, err = msgp.ReadArrayHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err)
		return o, err
	}
	if zb0001 != 4 {
		err = msgp.ArrayError{Wanted: 4, Got: zb0001}
		return o, err
	}
	bts, err = msgp.ReadExactBytes(bts, (x.VersionID)[:])
	if err != nil {
		err = msgp.WrapError(err, "VersionID")
		return o, err
	}
	x.ModTime, bts, err = msgp.ReadInt64Bytes(bts)
	if err != nil {
		err = msgp.WrapError(err, "ModTime")
		return o, err
	}
	{
		var zb0002 uint8
		zb0002, bts, err = msgp.ReadUint8Bytes(bts)
		if err != nil {
			err = msgp.WrapError(err, "Type")
			return o, err
		}
		x.Type = VersionType(zb0002)
	}
	{
		var zb0003 uint8
		zb0003, bts, err = msgp.ReadUint8Bytes(bts)
		if err != nil {
			err = msgp.WrapError(err, "Flags")
			return o, err
		}
		x.Flags = xlFlags(zb0003)
	}
	o = bts
	return o, err
}

// unmarshalV unmarshals with a specific metadata version.
func (j *xlMetaV2Version) unmarshalV(v uint8, bts []byte) (o []byte, err error) {
	if v > xlMetaVersion {
		return bts, fmt.Errorf("unknown xlMetaVersion: %d", v)
	}

	// Clear omitempty fields:
	if j.ObjectV2 != nil && len(j.ObjectV2.PartIndices) > 0 {
		j.ObjectV2.PartIndices = j.ObjectV2.PartIndices[:0]
	}
	o, err = j.UnmarshalMsg(bts)

	// Fix inconsistent x-minio-internal-replication-timestamp by converting to UTC.
	// Fixed in version 2 or later
	if err == nil && j.Type == DeleteType && v < 2 {
		if val, ok := j.DeleteMarker.MetaSys[ReservedMetadataPrefixLower+ReplicationTimestamp]; ok {
			tm, err := time.Parse(time.RFC3339Nano, string(val))
			if err == nil {
				j.DeleteMarker.MetaSys[ReservedMetadataPrefixLower+ReplicationTimestamp] = []byte(tm.UTC().Format(time.RFC3339Nano))
			}
		}
		if val, ok := j.DeleteMarker.MetaSys[ReservedMetadataPrefixLower+ReplicaTimestamp]; ok {
			tm, err := time.Parse(time.RFC3339Nano, string(val))
			if err == nil {
				j.DeleteMarker.MetaSys[ReservedMetadataPrefixLower+ReplicaTimestamp] = []byte(tm.UTC().Format(time.RFC3339Nano))
			}
		}
	}

	// Clean up PartEtags on v1
	if j.ObjectV2 != nil {
		allEmpty := true
		for _, tag := range j.ObjectV2.PartETags {
			if len(tag) != 0 {
				allEmpty = false
				break
			}
		}
		if allEmpty {
			j.ObjectV2.PartETags = nil
		}
	}
	return o, err
}

// xlMetaV2VersionHeaderV2 is a version 2 of xlMetaV2VersionHeader before EcN and EcM were added.
type xlMetaV2VersionHeaderV2 struct {
	*xlMetaV2VersionHeader
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *xlMetaV2VersionHeaderV2) UnmarshalMsg(bts []byte) (o []byte, err error) {
	z.EcN, z.EcN = 0, 0
	var zb0001 uint32
	zb0001, bts, err = msgp.ReadArrayHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err)
		return o, err
	}
	if zb0001 != 5 {
		err = msgp.ArrayError{Wanted: 5, Got: zb0001}
		return o, err
	}
	bts, err = msgp.ReadExactBytes(bts, (z.VersionID)[:])
	if err != nil {
		err = msgp.WrapError(err, "VersionID")
		return o, err
	}
	z.ModTime, bts, err = msgp.ReadInt64Bytes(bts)
	if err != nil {
		err = msgp.WrapError(err, "ModTime")
		return o, err
	}
	bts, err = msgp.ReadExactBytes(bts, (z.Signature)[:])
	if err != nil {
		err = msgp.WrapError(err, "Signature")
		return o, err
	}
	{
		var zb0002 uint8
		zb0002, bts, err = msgp.ReadUint8Bytes(bts)
		if err != nil {
			err = msgp.WrapError(err, "Type")
			return o, err
		}
		z.Type = VersionType(zb0002)
	}
	{
		var zb0003 uint8
		zb0003, bts, err = msgp.ReadUint8Bytes(bts)
		if err != nil {
			err = msgp.WrapError(err, "Flags")
			return o, err
		}
		z.Flags = xlFlags(zb0003)
	}
	o = bts
	return o, err
}

// DecodeMsg implements msgp.Decodable
func (z *xlMetaV2VersionHeaderV2) DecodeMsg(dc *msgp.Reader) (err error) {
	z.EcN, z.EcN = 0, 0
	var zb0001 uint32
	zb0001, err = dc.ReadArrayHeader()
	if err != nil {
		err = msgp.WrapError(err)
		return err
	}
	if zb0001 != 5 {
		err = msgp.ArrayError{Wanted: 5, Got: zb0001}
		return err
	}
	err = dc.ReadExactBytes((z.VersionID)[:])
	if err != nil {
		err = msgp.WrapError(err, "VersionID")
		return err
	}
	z.ModTime, err = dc.ReadInt64()
	if err != nil {
		err = msgp.WrapError(err, "ModTime")
		return err
	}
	err = dc.ReadExactBytes((z.Signature)[:])
	if err != nil {
		err = msgp.WrapError(err, "Signature")
		return err
	}
	{
		var zb0002 uint8
		zb0002, err = dc.ReadUint8()
		if err != nil {
			err = msgp.WrapError(err, "Type")
			return err
		}
		z.Type = VersionType(zb0002)
	}
	{
		var zb0003 uint8
		zb0003, err = dc.ReadUint8()
		if err != nil {
			err = msgp.WrapError(err, "Flags")
			return err
		}
		z.Flags = xlFlags(zb0003)
	}
	return err
}
