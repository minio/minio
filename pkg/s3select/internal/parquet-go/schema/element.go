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

package schema

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/minio/minio/pkg/s3select/internal/parquet-go/gen-go/parquet"
)

var nameRegexp = regexp.MustCompile("^[a-zA-Z0-9_]+$")

func validataPathSegments(pathSegments []string) error {
	for _, pathSegment := range pathSegments {
		if !nameRegexp.MatchString(pathSegment) {
			return fmt.Errorf("unsupported name %v", strings.Join(pathSegments, "."))
		}
	}

	return nil
}

// Element - represents schema element and its children. Any element must have Name and RepetitionType fields set.
type Element struct {
	parquet.SchemaElement
	numChildren        int32
	Encoding           *parquet.Encoding         // Optional; defaults is computed.
	CompressionType    *parquet.CompressionCodec // Optional; defaults to SNAPPY.
	Children           *Tree
	MaxDefinitionLevel int64
	MaxRepetitionLevel int64
	PathInTree         string
	PathInSchema       string
}

// String - stringify this element.
func (element *Element) String() string {
	var s []string
	s = append(s, "Name:"+element.Name)
	s = append(s, "RepetitionType:"+element.RepetitionType.String())
	if element.Type != nil {
		s = append(s, "Type:"+element.Type.String())
	}
	if element.ConvertedType != nil {
		s = append(s, "ConvertedType:"+element.ConvertedType.String())
	}
	if element.Encoding != nil {
		s = append(s, "Encoding:"+element.Encoding.String())
	}
	if element.CompressionType != nil {
		s = append(s, "CompressionType:"+element.CompressionType.String())
	}
	if element.Children != nil && element.Children.Length() > 0 {
		s = append(s, "Children:"+element.Children.String())
	}
	s = append(s, fmt.Sprintf("MaxDefinitionLevel:%v", element.MaxDefinitionLevel))
	s = append(s, fmt.Sprintf("MaxRepetitionLevel:%v", element.MaxRepetitionLevel))
	if element.PathInTree != "" {
		s = append(s, "PathInTree:"+element.PathInTree)
	}
	if element.PathInSchema != "" {
		s = append(s, "PathInSchema:"+element.PathInSchema)
	}

	return "{" + strings.Join(s, ", ") + "}"
}

// NewElement - creates new element.
func NewElement(name string, repetitionType parquet.FieldRepetitionType,
	elementType *parquet.Type, convertedType *parquet.ConvertedType,
	encoding *parquet.Encoding, compressionType *parquet.CompressionCodec,
	children *Tree) (*Element, error) {

	if !nameRegexp.MatchString(name) {
		return nil, fmt.Errorf("unsupported name %v", name)
	}

	switch repetitionType {
	case parquet.FieldRepetitionType_REQUIRED, parquet.FieldRepetitionType_OPTIONAL, parquet.FieldRepetitionType_REPEATED:
	default:
		return nil, fmt.Errorf("unknown repetition type %v", repetitionType)
	}

	if repetitionType == parquet.FieldRepetitionType_REPEATED && (elementType != nil || convertedType != nil) {
		return nil, fmt.Errorf("repetition type REPEATED should be used in group element")
	}

	if children != nil && children.Length() != 0 {
		if elementType != nil {
			return nil, fmt.Errorf("type should be nil for group element")
		}
	}

	element := Element{
		Encoding:        encoding,
		CompressionType: compressionType,
		Children:        children,
	}

	element.Name = name
	element.RepetitionType = &repetitionType
	element.Type = elementType
	element.ConvertedType = convertedType
	element.NumChildren = &element.numChildren
	if element.Children != nil {
		element.numChildren = int32(element.Children.Length())
	}

	return &element, nil
}
