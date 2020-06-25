/*
 * Minio Cloud Storage, (C) 2019 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package data

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/minio/minio/pkg/s3select/internal/parquet-go/common"
	"github.com/minio/minio/pkg/s3select/internal/parquet-go/encoding"
	"github.com/minio/minio/pkg/s3select/internal/parquet-go/gen-go/parquet"
	"github.com/minio/minio/pkg/s3select/internal/parquet-go/schema"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func getDefaultEncoding(parquetType parquet.Type) parquet.Encoding {
	switch parquetType {
	case parquet.Type_BOOLEAN:
		return parquet.Encoding_PLAIN
	case parquet.Type_INT32, parquet.Type_INT64, parquet.Type_FLOAT, parquet.Type_DOUBLE:
		return parquet.Encoding_RLE_DICTIONARY
	case parquet.Type_BYTE_ARRAY:
		return parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY
	}

	return parquet.Encoding_PLAIN
}

func getFirstValueElement(tree *schema.Tree) (valueElement *schema.Element) {
	tree.Range(func(name string, element *schema.Element) bool {
		if element.Children == nil {
			valueElement = element
		} else {
			valueElement = getFirstValueElement(element.Children)
		}

		return false
	})

	return valueElement
}

func populate(columnDataMap map[string]*Column, input *jsonValue, tree *schema.Tree, firstValueRL int64) (map[string]*Column, error) {
	var err error

	pos := 0
	handleElement := func(name string, element *schema.Element) bool {
		pos++

		dataPath := element.PathInTree

		if *element.RepetitionType == parquet.FieldRepetitionType_REPEATED {
			panic(fmt.Errorf("%v: repetition type must be REQUIRED or OPTIONAL type", dataPath))
		}

		inputValue := input.Get(name)
		if *element.RepetitionType == parquet.FieldRepetitionType_REQUIRED && inputValue.IsNull() {
			err = fmt.Errorf("%v: nil value for required field", dataPath)
			return false
		}

		add := func(element *schema.Element, value interface{}, DL, RL int64) {
			columnData := columnDataMap[element.PathInSchema]
			if columnData == nil {
				columnData = NewColumn(*element.Type)
			}
			columnData.add(value, DL, RL)
			columnDataMap[element.PathInSchema] = columnData
		}

		// Handle primitive type element.
		if element.Type != nil {
			var value interface{}
			if value, err = inputValue.GetValue(*element.Type, element.ConvertedType); err != nil {
				return false
			}

			DL := element.MaxDefinitionLevel
			if value == nil && DL > 0 {
				DL--
			}

			RL := element.MaxRepetitionLevel
			if pos == 1 {
				RL = firstValueRL
			}

			add(element, value, DL, RL)
			return true
		}

		addNull := func() {
			valueElement := getFirstValueElement(element.Children)

			DL := element.MaxDefinitionLevel
			if DL > 0 {
				DL--
			}

			RL := element.MaxRepetitionLevel
			if RL > 0 {
				RL--
			}

			add(valueElement, nil, DL, RL)
		}

		// Handle group type element.
		if element.ConvertedType == nil {
			if inputValue.IsNull() {
				addNull()
				return true
			}

			columnDataMap, err = populate(columnDataMap, inputValue, element.Children, firstValueRL)
			return (err == nil)
		}

		// Handle list type element.
		if *element.ConvertedType == parquet.ConvertedType_LIST {
			if inputValue.IsNull() {
				addNull()
				return true
			}

			var results []gjson.Result
			if results, err = inputValue.GetArray(); err != nil {
				return false
			}

			listElement, _ := element.Children.Get("list")
			valueElement, _ := listElement.Children.Get("element")
			for i := range results {
				rl := valueElement.MaxRepetitionLevel
				if i == 0 {
					rl = firstValueRL
				}

				var jsonData []byte
				if jsonData, err = sjson.SetBytes([]byte{}, "element", results[i].Value()); err != nil {
					return false
				}

				var jv *jsonValue
				if jv, err = bytesToJSONValue(jsonData); err != nil {
					return false
				}

				if columnDataMap, err = populate(columnDataMap, jv, listElement.Children, rl); err != nil {
					return false
				}
			}
			return true
		}

		if *element.ConvertedType == parquet.ConvertedType_MAP {
			if inputValue.IsNull() {
				addNull()
				return true
			}

			keyValueElement, _ := element.Children.Get("key_value")
			var rerr error
			err = inputValue.Range(func(key, value gjson.Result) bool {
				if !key.Exists() || key.Type == gjson.Null {
					rerr = fmt.Errorf("%v.key_value.key: not found or null", dataPath)
					return false
				}

				var jsonData []byte
				if jsonData, rerr = sjson.SetBytes([]byte{}, "key", key.Value()); err != nil {
					return false
				}

				if jsonData, rerr = sjson.SetBytes(jsonData, "value", value.Value()); err != nil {
					return false
				}

				var jv *jsonValue
				if jv, rerr = bytesToJSONValue(jsonData); rerr != nil {
					return false
				}

				if columnDataMap, rerr = populate(columnDataMap, jv, keyValueElement.Children, firstValueRL); err != nil {
					return false
				}

				return true
			})

			if err != nil {
				return false
			}

			err = rerr
			return (err == nil)
		}

		err = fmt.Errorf("%v: unsupported converted type %v in %v field type", dataPath, *element.ConvertedType, *element.RepetitionType)
		return false
	}

	tree.Range(handleElement)
	return columnDataMap, err
}

// Column - denotes values of a column.
type Column struct {
	parquetType      parquet.Type  // value type.
	values           []interface{} // must be a slice of parquet typed values.
	definitionLevels []int64       // exactly same length of values.
	repetitionLevels []int64       // exactly same length of values.
	rowCount         int32
	maxBitWidth      int32
	minValue         interface{}
	maxValue         interface{}
}

func (column *Column) updateMinMaxValue(value interface{}) {
	if column.minValue == nil && column.maxValue == nil {
		column.minValue = value
		column.maxValue = value
		return
	}

	switch column.parquetType {
	case parquet.Type_BOOLEAN:
		if column.minValue.(bool) && !value.(bool) {
			column.minValue = value
		}

		if !column.maxValue.(bool) && value.(bool) {
			column.maxValue = value
		}

	case parquet.Type_INT32:
		if column.minValue.(int32) > value.(int32) {
			column.minValue = value
		}

		if column.maxValue.(int32) < value.(int32) {
			column.maxValue = value
		}

	case parquet.Type_INT64:
		if column.minValue.(int64) > value.(int64) {
			column.minValue = value
		}

		if column.maxValue.(int64) < value.(int64) {
			column.maxValue = value
		}

	case parquet.Type_FLOAT:
		if column.minValue.(float32) > value.(float32) {
			column.minValue = value
		}

		if column.maxValue.(float32) < value.(float32) {
			column.maxValue = value
		}

	case parquet.Type_DOUBLE:
		if column.minValue.(float64) > value.(float64) {
			column.minValue = value
		}

		if column.maxValue.(float64) < value.(float64) {
			column.maxValue = value
		}

	case parquet.Type_BYTE_ARRAY:
		if bytes.Compare(column.minValue.([]byte), value.([]byte)) > 0 {
			column.minValue = value
		}

		if bytes.Compare(column.minValue.([]byte), value.([]byte)) < 0 {
			column.maxValue = value
		}
	}
}

func (column *Column) updateStats(value interface{}, DL, RL int64) {
	if RL == 0 {
		column.rowCount++
	}

	if value == nil {
		return
	}

	var bitWidth int32
	switch column.parquetType {
	case parquet.Type_BOOLEAN:
		bitWidth = 1
	case parquet.Type_INT32:
		bitWidth = common.BitWidth(uint64(value.(int32)))
	case parquet.Type_INT64:
		bitWidth = common.BitWidth(uint64(value.(int64)))
	case parquet.Type_FLOAT:
		bitWidth = 32
	case parquet.Type_DOUBLE:
		bitWidth = 64
	case parquet.Type_BYTE_ARRAY:
		bitWidth = int32(len(value.([]byte)))
	}
	if column.maxBitWidth < bitWidth {
		column.maxBitWidth = bitWidth
	}

	column.updateMinMaxValue(value)
}

func (column *Column) add(value interface{}, DL, RL int64) {
	column.values = append(column.values, value)
	column.definitionLevels = append(column.definitionLevels, DL)
	column.repetitionLevels = append(column.repetitionLevels, RL)
	column.updateStats(value, DL, RL)
}

// AddNull - adds nil value.
func (column *Column) AddNull(DL, RL int64) {
	column.add(nil, DL, RL)
}

// AddBoolean - adds boolean value.
func (column *Column) AddBoolean(value bool, DL, RL int64) {
	if column.parquetType != parquet.Type_BOOLEAN {
		panic(fmt.Errorf("expected %v value", column.parquetType))
	}

	column.add(value, DL, RL)
}

// AddInt32 - adds int32 value.
func (column *Column) AddInt32(value int32, DL, RL int64) {
	if column.parquetType != parquet.Type_INT32 {
		panic(fmt.Errorf("expected %v value", column.parquetType))
	}

	column.add(value, DL, RL)
}

// AddInt64 - adds int64 value.
func (column *Column) AddInt64(value int64, DL, RL int64) {
	if column.parquetType != parquet.Type_INT64 {
		panic(fmt.Errorf("expected %v value", column.parquetType))
	}

	column.add(value, DL, RL)
}

// AddFloat - adds float32 value.
func (column *Column) AddFloat(value float32, DL, RL int64) {
	if column.parquetType != parquet.Type_FLOAT {
		panic(fmt.Errorf("expected %v value", column.parquetType))
	}

	column.add(value, DL, RL)
}

// AddDouble - adds float64 value.
func (column *Column) AddDouble(value float64, DL, RL int64) {
	if column.parquetType != parquet.Type_DOUBLE {
		panic(fmt.Errorf("expected %v value", column.parquetType))
	}

	column.add(value, DL, RL)
}

// AddByteArray - adds byte array value.
func (column *Column) AddByteArray(value []byte, DL, RL int64) {
	if column.parquetType != parquet.Type_BYTE_ARRAY {
		panic(fmt.Errorf("expected %v value", column.parquetType))
	}

	column.add(value, DL, RL)
}

// Merge - merges columns.
func (column *Column) Merge(column2 *Column) {
	if column.parquetType != column2.parquetType {
		panic(fmt.Errorf("merge differs in parquet type"))
	}

	column.values = append(column.values, column2.values...)
	column.definitionLevels = append(column.definitionLevels, column2.definitionLevels...)
	column.repetitionLevels = append(column.repetitionLevels, column2.repetitionLevels...)

	column.rowCount += column2.rowCount
	if column.maxBitWidth < column2.maxBitWidth {
		column.maxBitWidth = column2.maxBitWidth
	}

	column.updateMinMaxValue(column2.minValue)
	column.updateMinMaxValue(column2.maxValue)
}

func (column *Column) String() string {
	var strs []string
	strs = append(strs, fmt.Sprintf("parquetType: %v", column.parquetType))
	strs = append(strs, fmt.Sprintf("values: %v", column.values))
	strs = append(strs, fmt.Sprintf("definitionLevels: %v", column.definitionLevels))
	strs = append(strs, fmt.Sprintf("repetitionLevels: %v", column.repetitionLevels))
	strs = append(strs, fmt.Sprintf("rowCount: %v", column.rowCount))
	strs = append(strs, fmt.Sprintf("maxBitWidth: %v", column.maxBitWidth))
	strs = append(strs, fmt.Sprintf("minValue: %v", column.minValue))
	strs = append(strs, fmt.Sprintf("maxValue: %v", column.maxValue))
	return "{" + strings.Join(strs, ", ") + "}"
}

func (column *Column) encodeValue(value interface{}, element *schema.Element) []byte {
	if value == nil {
		return nil
	}

	valueData := encoding.PlainEncode(common.ToSliceValue([]interface{}{value}, column.parquetType), column.parquetType)
	if column.parquetType == parquet.Type_BYTE_ARRAY && element.ConvertedType != nil {
		switch *element.ConvertedType {
		case parquet.ConvertedType_UTF8, parquet.ConvertedType_DECIMAL:
			valueData = valueData[4:]
		}
	}

	return valueData
}

func (column *Column) toDataPageV2(element *schema.Element, parquetEncoding parquet.Encoding) *ColumnChunk {
	var definedValues []interface{}
	for _, value := range column.values {
		if value != nil {
			definedValues = append(definedValues, value)
		}
	}

	var encodedData []byte
	switch parquetEncoding {
	case parquet.Encoding_PLAIN:
		encodedData = encoding.PlainEncode(common.ToSliceValue(definedValues, column.parquetType), column.parquetType)

	case parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY:
		var bytesSlices [][]byte
		for _, value := range column.values {
			bytesSlices = append(bytesSlices, value.([]byte))
		}
		encodedData = encoding.DeltaLengthByteArrayEncode(bytesSlices)
	}

	compressionType := parquet.CompressionCodec_SNAPPY
	if element.CompressionType != nil {
		compressionType = *element.CompressionType
	}

	compressedData, err := common.Compress(compressionType, encodedData)
	if err != nil {
		panic(err)
	}

	DLData := encoding.RLEBitPackedHybridEncode(
		column.definitionLevels,
		common.BitWidth(uint64(element.MaxDefinitionLevel)),
		parquet.Type_INT64,
	)

	RLData := encoding.RLEBitPackedHybridEncode(
		column.repetitionLevels,
		common.BitWidth(uint64(element.MaxRepetitionLevel)),
		parquet.Type_INT64,
	)

	pageHeader := parquet.NewPageHeader()
	pageHeader.Type = parquet.PageType_DATA_PAGE_V2
	pageHeader.CompressedPageSize = int32(len(compressedData) + len(DLData) + len(RLData))
	pageHeader.UncompressedPageSize = int32(len(encodedData) + len(DLData) + len(RLData))
	pageHeader.DataPageHeaderV2 = parquet.NewDataPageHeaderV2()
	pageHeader.DataPageHeaderV2.NumValues = int32(len(column.values))
	pageHeader.DataPageHeaderV2.NumNulls = int32(len(column.values) - len(definedValues))
	pageHeader.DataPageHeaderV2.NumRows = column.rowCount
	pageHeader.DataPageHeaderV2.Encoding = parquetEncoding
	pageHeader.DataPageHeaderV2.DefinitionLevelsByteLength = int32(len(DLData))
	pageHeader.DataPageHeaderV2.RepetitionLevelsByteLength = int32(len(RLData))
	pageHeader.DataPageHeaderV2.IsCompressed = true
	pageHeader.DataPageHeaderV2.Statistics = parquet.NewStatistics()
	pageHeader.DataPageHeaderV2.Statistics.Min = column.encodeValue(column.minValue, element)
	pageHeader.DataPageHeaderV2.Statistics.Max = column.encodeValue(column.maxValue, element)

	ts := thrift.NewTSerializer()
	ts.Protocol = thrift.NewTCompactProtocolFactory().GetProtocol(ts.Transport)
	rawData, err := ts.Write(context.TODO(), pageHeader)
	if err != nil {
		panic(err)
	}
	rawData = append(rawData, RLData...)
	rawData = append(rawData, DLData...)
	rawData = append(rawData, compressedData...)

	metadata := parquet.NewColumnMetaData()
	metadata.Type = column.parquetType
	metadata.Encodings = []parquet.Encoding{
		parquet.Encoding_PLAIN,
		parquet.Encoding_RLE,
		parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY,
	}
	metadata.Codec = compressionType
	metadata.NumValues = int64(pageHeader.DataPageHeaderV2.NumValues)
	metadata.TotalCompressedSize = int64(len(rawData))
	metadata.TotalUncompressedSize = int64(pageHeader.UncompressedPageSize) + int64(len(rawData)) - int64(pageHeader.CompressedPageSize)
	metadata.PathInSchema = strings.Split(element.PathInSchema, ".")
	metadata.Statistics = parquet.NewStatistics()
	metadata.Statistics.Min = pageHeader.DataPageHeaderV2.Statistics.Min
	metadata.Statistics.Max = pageHeader.DataPageHeaderV2.Statistics.Max

	chunk := new(ColumnChunk)
	chunk.ColumnChunk.MetaData = metadata
	chunk.dataPageLen = int64(len(rawData))
	chunk.dataLen = int64(len(rawData))
	chunk.data = rawData

	return chunk
}

func (column *Column) toRLEDictPage(element *schema.Element) *ColumnChunk {
	dictPageData, dataPageData, dictValueCount, indexBitWidth := encoding.RLEDictEncode(column.values, column.parquetType, column.maxBitWidth)

	compressionType := parquet.CompressionCodec_SNAPPY
	if element.CompressionType != nil {
		compressionType = *element.CompressionType
	}

	compressedData, err := common.Compress(compressionType, dictPageData)
	if err != nil {
		panic(err)
	}

	dictPageHeader := parquet.NewPageHeader()
	dictPageHeader.Type = parquet.PageType_DICTIONARY_PAGE
	dictPageHeader.CompressedPageSize = int32(len(compressedData))
	dictPageHeader.UncompressedPageSize = int32(len(dictPageData))
	dictPageHeader.DictionaryPageHeader = parquet.NewDictionaryPageHeader()
	dictPageHeader.DictionaryPageHeader.NumValues = dictValueCount
	dictPageHeader.DictionaryPageHeader.Encoding = parquet.Encoding_PLAIN

	ts := thrift.NewTSerializer()
	ts.Protocol = thrift.NewTCompactProtocolFactory().GetProtocol(ts.Transport)
	dictPageRawData, err := ts.Write(context.TODO(), dictPageHeader)
	if err != nil {
		panic(err)
	}
	dictPageRawData = append(dictPageRawData, compressedData...)

	RLData := encoding.RLEBitPackedHybridEncode(
		column.repetitionLevels,
		common.BitWidth(uint64(element.MaxRepetitionLevel)),
		parquet.Type_INT64,
	)
	encodedData := RLData

	DLData := encoding.RLEBitPackedHybridEncode(
		column.definitionLevels,
		common.BitWidth(uint64(element.MaxDefinitionLevel)),
		parquet.Type_INT64,
	)
	encodedData = append(encodedData, DLData...)

	encodedData = append(encodedData, indexBitWidth)
	encodedData = append(encodedData, dataPageData...)

	compressedData, err = common.Compress(compressionType, encodedData)
	if err != nil {
		panic(err)
	}

	dataPageHeader := parquet.NewPageHeader()
	dataPageHeader.Type = parquet.PageType_DATA_PAGE
	dataPageHeader.CompressedPageSize = int32(len(compressedData))
	dataPageHeader.UncompressedPageSize = int32(len(encodedData))
	dataPageHeader.DataPageHeader = parquet.NewDataPageHeader()
	dataPageHeader.DataPageHeader.NumValues = int32(len(column.values))
	dataPageHeader.DataPageHeader.DefinitionLevelEncoding = parquet.Encoding_RLE
	dataPageHeader.DataPageHeader.RepetitionLevelEncoding = parquet.Encoding_RLE
	dataPageHeader.DataPageHeader.Encoding = parquet.Encoding_RLE_DICTIONARY

	ts = thrift.NewTSerializer()
	ts.Protocol = thrift.NewTCompactProtocolFactory().GetProtocol(ts.Transport)
	dataPageRawData, err := ts.Write(context.TODO(), dataPageHeader)
	if err != nil {
		panic(err)
	}
	dataPageRawData = append(dataPageRawData, compressedData...)

	metadata := parquet.NewColumnMetaData()
	metadata.Type = column.parquetType
	metadata.Encodings = []parquet.Encoding{
		parquet.Encoding_PLAIN,
		parquet.Encoding_RLE,
		parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY,
		parquet.Encoding_RLE_DICTIONARY,
	}
	metadata.Codec = compressionType
	metadata.NumValues = int64(dataPageHeader.DataPageHeader.NumValues)
	metadata.TotalCompressedSize = int64(len(dictPageRawData)) + int64(len(dataPageRawData))
	uncompressedSize := int64(dictPageHeader.UncompressedPageSize) + int64(len(dictPageData)) - int64(dictPageHeader.CompressedPageSize)
	uncompressedSize += int64(dataPageHeader.UncompressedPageSize) + int64(len(dataPageData)) - int64(dataPageHeader.CompressedPageSize)
	metadata.TotalUncompressedSize = uncompressedSize
	metadata.PathInSchema = strings.Split(element.PathInSchema, ".")
	metadata.Statistics = parquet.NewStatistics()
	metadata.Statistics.Min = column.encodeValue(column.minValue, element)
	metadata.Statistics.Max = column.encodeValue(column.maxValue, element)

	chunk := new(ColumnChunk)
	chunk.ColumnChunk.MetaData = metadata
	chunk.isDictPage = true
	chunk.dictPageLen = int64(len(dictPageRawData))
	chunk.dataPageLen = int64(len(dataPageRawData))
	chunk.dataLen = chunk.dictPageLen + chunk.dataPageLen
	chunk.data = append(dictPageRawData, dataPageRawData...)

	return chunk
}

// Encode an element.
func (column *Column) Encode(element *schema.Element) *ColumnChunk {
	parquetEncoding := getDefaultEncoding(column.parquetType)
	if element.Encoding != nil {
		parquetEncoding = *element.Encoding
	}

	switch parquetEncoding {
	case parquet.Encoding_PLAIN, parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY:
		return column.toDataPageV2(element, parquetEncoding)
	}

	return column.toRLEDictPage(element)
}

// NewColumn - creates new column data
func NewColumn(parquetType parquet.Type) *Column {
	switch parquetType {
	case parquet.Type_BOOLEAN, parquet.Type_INT32, parquet.Type_INT64, parquet.Type_FLOAT, parquet.Type_DOUBLE, parquet.Type_BYTE_ARRAY:
	default:
		panic(fmt.Errorf("unsupported parquet type %v", parquetType))
	}

	return &Column{
		parquetType: parquetType,
	}
}

// UnmarshalJSON - decodes JSON data into map of Column.
func UnmarshalJSON(data []byte, tree *schema.Tree) (map[string]*Column, error) {
	if !tree.ReadOnly() {
		return nil, fmt.Errorf("tree must be read only")
	}

	inputValue, err := bytesToJSONValue(data)
	if err != nil {
		return nil, err
	}

	columnDataMap := make(map[string]*Column)
	return populate(columnDataMap, inputValue, tree, 0)
}
