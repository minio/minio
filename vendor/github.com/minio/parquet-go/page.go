/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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

package parquet

import (
	"bytes"
	"fmt"
	"strings"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/minio/parquet-go/gen-go/parquet"
)

// getBitWidth - returns bits required to place num e.g.
//
//    num | width
//   -----|-------
//     0  |   0
//     1  |   1
//     2  |   2
//     3  |   2
//     4  |   3
//     5  |   3
//    ... |  ...
//    ... |  ...
//
func getBitWidth(num uint64) (width uint64) {
	for ; num != 0; num >>= 1 {
		width++
	}

	return width
}

// getMaxDefLevel - get maximum definition level.
func getMaxDefLevel(nameIndexMap map[string]int, schemaElements []*parquet.SchemaElement, path []string) (v int) {
	for i := 1; i <= len(path); i++ {
		name := strings.Join(path[:i], ".")
		if index, ok := nameIndexMap[name]; ok {
			if schemaElements[index].GetRepetitionType() != parquet.FieldRepetitionType_REQUIRED {
				v++
			}
		}
	}

	return v
}

// getMaxRepLevel - get maximum repetition level.
func getMaxRepLevel(nameIndexMap map[string]int, schemaElements []*parquet.SchemaElement, path []string) (v int) {
	for i := 1; i <= len(path); i++ {
		name := strings.Join(path[:i], ".")
		if index, ok := nameIndexMap[name]; ok {
			if schemaElements[index].GetRepetitionType() == parquet.FieldRepetitionType_REPEATED {
				v++
			}
		}
	}

	return v
}

func readPageHeader(reader *thrift.TBufferedTransport) (*parquet.PageHeader, error) {
	pageHeader := parquet.NewPageHeader()
	if err := pageHeader.Read(thrift.NewTCompactProtocol(reader)); err != nil {
		return nil, err
	}

	return pageHeader, nil
}

func readPageRawData(thriftReader *thrift.TBufferedTransport, metadata *parquet.ColumnMetaData) (page *page, err error) {
	pageHeader, err := readPageHeader(thriftReader)
	if err != nil {
		return nil, err
	}

	switch pageType := pageHeader.GetType(); pageType {
	case parquet.PageType_DICTIONARY_PAGE:
		page = newDictPage()
	case parquet.PageType_DATA_PAGE, parquet.PageType_DATA_PAGE_V2:
		page = newDataPage()
	default:
		return nil, fmt.Errorf("unsupported page type %v", pageType)
	}

	compressedPageSize := pageHeader.GetCompressedPageSize()
	buf := make([]byte, compressedPageSize)
	if _, err := thriftReader.Read(buf); err != nil {
		return nil, err
	}

	page.Header = pageHeader
	page.CompressType = metadata.GetCodec()
	page.RawData = buf
	page.Path = append([]string{}, metadata.GetPathInSchema()...)
	page.DataType = metadata.GetType()

	return page, nil
}

func readPage(
	thriftReader *thrift.TBufferedTransport,
	metadata *parquet.ColumnMetaData,
	columnNameIndexMap map[string]int,
	schemaElements []*parquet.SchemaElement,
) (page *page, definitionLevels, numRows int64, err error) {

	pageHeader, err := readPageHeader(thriftReader)
	if err != nil {
		return nil, 0, 0, err
	}

	read := func() (data []byte, err error) {
		var repLevelsLen, defLevelsLen int32
		var repLevelsBuf, defLevelsBuf []byte

		if pageHeader.GetType() == parquet.PageType_DATA_PAGE_V2 {
			repLevelsLen = pageHeader.DataPageHeaderV2.GetRepetitionLevelsByteLength()
			repLevelsBuf = make([]byte, repLevelsLen)
			if _, err = thriftReader.Read(repLevelsBuf); err != nil {
				return nil, err
			}

			defLevelsLen = pageHeader.DataPageHeaderV2.GetDefinitionLevelsByteLength()
			defLevelsBuf = make([]byte, defLevelsLen)
			if _, err = thriftReader.Read(defLevelsBuf); err != nil {
				return nil, err
			}
		}

		dataBuf := make([]byte, pageHeader.GetCompressedPageSize()-repLevelsLen-defLevelsLen)
		if _, err = thriftReader.Read(dataBuf); err != nil {
			return nil, err
		}

		if dataBuf, err = compressionCodec(metadata.GetCodec()).uncompress(dataBuf); err != nil {
			return nil, err
		}

		if repLevelsLen == 0 && defLevelsLen == 0 {
			return dataBuf, nil
		}

		if repLevelsLen > 0 {
			data = append(data, uint32ToBytes(uint32(repLevelsLen))...)
			data = append(data, repLevelsBuf...)
		}

		if defLevelsLen > 0 {
			data = append(data, uint32ToBytes(uint32(defLevelsLen))...)
			data = append(data, defLevelsBuf...)
		}

		data = append(data, dataBuf...)

		return data, nil
	}

	buf, err := read()
	if err != nil {
		return nil, 0, 0, err
	}

	path := append([]string{}, metadata.GetPathInSchema()...)

	bytesReader := bytes.NewReader(buf)
	pageType := pageHeader.GetType()
	switch pageType {
	case parquet.PageType_INDEX_PAGE:
		return nil, 0, 0, fmt.Errorf("page type %v is not supported", parquet.PageType_INDEX_PAGE)

	case parquet.PageType_DICTIONARY_PAGE:
		page = newDictPage()
		page.Header = pageHeader
		table := new(table)
		table.Path = path
		values, err := readValues(bytesReader, metadata.GetType(),
			uint64(pageHeader.DictionaryPageHeader.GetNumValues()), 0)
		if err != nil {
			return nil, 0, 0, err
		}
		table.Values = getTableValues(values, metadata.GetType())
		page.DataTable = table

		return page, 0, 0, nil

	case parquet.PageType_DATA_PAGE, parquet.PageType_DATA_PAGE_V2:
		name := strings.Join(path, ".")

		page = newDataPage()
		page.Header = pageHeader

		maxDefinitionLevel := getMaxDefLevel(columnNameIndexMap, schemaElements, path)
		maxRepetitionLevel := getMaxRepLevel(columnNameIndexMap, schemaElements, path)

		var numValues uint64
		var encodingType parquet.Encoding

		if pageHeader.GetType() == parquet.PageType_DATA_PAGE {
			numValues = uint64(pageHeader.DataPageHeader.GetNumValues())
			encodingType = pageHeader.DataPageHeader.GetEncoding()
		} else {
			numValues = uint64(pageHeader.DataPageHeaderV2.GetNumValues())
			encodingType = pageHeader.DataPageHeaderV2.GetEncoding()
		}

		var repetitionLevels []int64
		if maxRepetitionLevel > 0 {
			values, _, err := readDataPageValues(bytesReader, parquet.Encoding_RLE, parquet.Type_INT64,
				-1, numValues, getBitWidth(uint64(maxRepetitionLevel)))
			if err != nil {
				return nil, 0, 0, err
			}

			if repetitionLevels = values.([]int64); uint64(len(repetitionLevels)) > numValues {
				repetitionLevels = repetitionLevels[:numValues]
			}
		} else {
			repetitionLevels = make([]int64, numValues)
		}

		var definitionLevels []int64
		if maxDefinitionLevel > 0 {
			values, _, err := readDataPageValues(bytesReader, parquet.Encoding_RLE, parquet.Type_INT64,
				-1, numValues, getBitWidth(uint64(maxDefinitionLevel)))
			if err != nil {
				return nil, 0, 0, err
			}
			if definitionLevels = values.([]int64); uint64(len(definitionLevels)) > numValues {
				definitionLevels = definitionLevels[:numValues]
			}
		} else {
			definitionLevels = make([]int64, numValues)
		}

		var numNulls uint64
		for i := 0; i < len(definitionLevels); i++ {
			if definitionLevels[i] != int64(maxDefinitionLevel) {
				numNulls++
			}
		}

		var convertedType parquet.ConvertedType = -1
		if schemaElements[columnNameIndexMap[name]].IsSetConvertedType() {
			convertedType = schemaElements[columnNameIndexMap[name]].GetConvertedType()
		}
		values, valueType, err := readDataPageValues(bytesReader, encodingType, metadata.GetType(),
			convertedType, uint64(len(definitionLevels))-numNulls,
			uint64(schemaElements[columnNameIndexMap[name]].GetTypeLength()))
		if err != nil {
			return nil, 0, 0, err
		}
		tableValues := getTableValues(values, valueType)

		table := new(table)
		table.Path = path
		table.RepetitionType = schemaElements[columnNameIndexMap[name]].GetRepetitionType()
		table.MaxRepetitionLevel = int32(maxRepetitionLevel)
		table.MaxDefinitionLevel = int32(maxDefinitionLevel)
		table.Values = make([]interface{}, len(definitionLevels))
		table.RepetitionLevels = make([]int32, len(definitionLevels))
		table.DefinitionLevels = make([]int32, len(definitionLevels))

		j := 0
		numRows := int64(0)
		for i := 0; i < len(definitionLevels); i++ {
			table.RepetitionLevels[i] = int32(repetitionLevels[i])
			table.DefinitionLevels[i] = int32(definitionLevels[i])
			if int(table.DefinitionLevels[i]) == maxDefinitionLevel {
				table.Values[i] = tableValues[j]
				j++
			}
			if table.RepetitionLevels[i] == 0 {
				numRows++
			}
		}
		page.DataTable = table

		return page, int64(len(definitionLevels)), numRows, nil
	}

	return nil, 0, 0, fmt.Errorf("unknown page type %v", pageType)
}

type page struct {
	Header       *parquet.PageHeader      // Header of a page
	DataTable    *table                   // Table to store values
	RawData      []byte                   // Compressed data of the page, which is written in parquet file
	CompressType parquet.CompressionCodec // Compress type: gzip/snappy/none
	DataType     parquet.Type             // Parquet type of the values in the page
	Path         []string                 // Path in schema(include the root)
	MaxVal       interface{}              // Maximum of the values
	MinVal       interface{}              // Minimum of the values
	PageSize     int32
}

func newPage() *page {
	return &page{
		Header:   parquet.NewPageHeader(),
		PageSize: 8 * 1024,
	}
}

func newDictPage() *page {
	page := newPage()
	page.Header.DictionaryPageHeader = parquet.NewDictionaryPageHeader()
	return page
}

func newDataPage() *page {
	page := newPage()
	page.Header.DataPageHeader = parquet.NewDataPageHeader()
	return page
}

func (page *page) decode(dictPage *page) {
	if dictPage == nil || page == nil || page.Header.DataPageHeader == nil ||
		(page.Header.DataPageHeader.Encoding != parquet.Encoding_RLE_DICTIONARY &&
			page.Header.DataPageHeader.Encoding != parquet.Encoding_PLAIN_DICTIONARY) {
		return
	}

	for i := 0; i < len(page.DataTable.Values); i++ {
		if page.DataTable.Values[i] != nil {
			index := page.DataTable.Values[i].(int64)
			page.DataTable.Values[i] = dictPage.DataTable.Values[index]
		}
	}
}

// Get RepetitionLevels and Definitions from RawData
func (page *page) getRLDLFromRawData(columnNameIndexMap map[string]int, schemaElements []*parquet.SchemaElement) (numValues int64, numRows int64, err error) {
	bytesReader := bytes.NewReader(page.RawData)

	pageType := page.Header.GetType()

	var buf []byte
	if pageType == parquet.PageType_DATA_PAGE_V2 {
		var repLevelsLen, defLevelsLen int32
		var repLevelsBuf, defLevelsBuf []byte

		repLevelsLen = page.Header.DataPageHeaderV2.GetRepetitionLevelsByteLength()
		repLevelsBuf = make([]byte, repLevelsLen)
		if _, err = bytesReader.Read(repLevelsBuf); err != nil {
			return 0, 0, err
		}

		defLevelsLen = page.Header.DataPageHeaderV2.GetDefinitionLevelsByteLength()
		defLevelsBuf = make([]byte, defLevelsLen)
		if _, err = bytesReader.Read(defLevelsBuf); err != nil {
			return 0, 0, err
		}

		dataBuf := make([]byte, len(page.RawData)-int(repLevelsLen)-int(defLevelsLen))
		if _, err = bytesReader.Read(dataBuf); err != nil {
			return 0, 0, err
		}

		if repLevelsLen == 0 && defLevelsLen == 0 {
			buf = dataBuf
		} else {
			if repLevelsLen > 0 {
				buf = append(buf, uint32ToBytes(uint32(repLevelsLen))...)
				buf = append(buf, repLevelsBuf...)
			}

			if defLevelsLen > 0 {
				buf = append(buf, uint32ToBytes(uint32(defLevelsLen))...)
				buf = append(buf, defLevelsBuf...)
			}

			buf = append(buf, dataBuf...)
		}
	} else {
		if buf, err = compressionCodec(page.CompressType).uncompress(page.RawData); err != nil {
			return 0, 0, err
		}
	}

	bytesReader = bytes.NewReader(buf)

	switch pageType {
	case parquet.PageType_DICTIONARY_PAGE:
		table := new(table)
		table.Path = page.Path
		page.DataTable = table
		return 0, 0, nil

	case parquet.PageType_DATA_PAGE, parquet.PageType_DATA_PAGE_V2:
		var numValues uint64
		if pageType == parquet.PageType_DATA_PAGE {
			numValues = uint64(page.Header.DataPageHeader.GetNumValues())
		} else {
			numValues = uint64(page.Header.DataPageHeaderV2.GetNumValues())
		}

		maxDefinitionLevel := getMaxDefLevel(columnNameIndexMap, schemaElements, page.Path)
		maxRepetitionLevel := getMaxRepLevel(columnNameIndexMap, schemaElements, page.Path)

		var repetitionLevels []int64
		if maxRepetitionLevel > 0 {
			values, _, err := readDataPageValues(bytesReader, parquet.Encoding_RLE, parquet.Type_INT64,
				-1, numValues, getBitWidth(uint64(maxRepetitionLevel)))
			if err != nil {
				return 0, 0, err
			}

			if repetitionLevels = values.([]int64); uint64(len(repetitionLevels)) > numValues {
				repetitionLevels = repetitionLevels[:numValues]
			}
		} else {
			repetitionLevels = make([]int64, numValues)
		}

		var definitionLevels []int64
		if maxDefinitionLevel > 0 {
			values, _, err := readDataPageValues(bytesReader, parquet.Encoding_RLE, parquet.Type_INT64,
				-1, numValues, getBitWidth(uint64(maxDefinitionLevel)))
			if err != nil {
				return 0, 0, err
			}
			if definitionLevels = values.([]int64); uint64(len(definitionLevels)) > numValues {
				definitionLevels = definitionLevels[:numValues]
			}
		} else {
			definitionLevels = make([]int64, numValues)
		}

		table := new(table)
		table.Path = page.Path
		name := strings.Join(page.Path, ".")
		table.RepetitionType = schemaElements[columnNameIndexMap[name]].GetRepetitionType()
		table.MaxRepetitionLevel = int32(maxRepetitionLevel)
		table.MaxDefinitionLevel = int32(maxDefinitionLevel)
		table.Values = make([]interface{}, len(definitionLevels))
		table.RepetitionLevels = make([]int32, len(definitionLevels))
		table.DefinitionLevels = make([]int32, len(definitionLevels))

		numRows := int64(0)
		for i := 0; i < len(definitionLevels); i++ {
			table.RepetitionLevels[i] = int32(repetitionLevels[i])
			table.DefinitionLevels[i] = int32(definitionLevels[i])
			if table.RepetitionLevels[i] == 0 {
				numRows++
			}
		}
		page.DataTable = table
		page.RawData = buf[len(buf)-bytesReader.Len():]

		return int64(numValues), numRows, nil
	}

	return 0, 0, fmt.Errorf("Unsupported page type %v", pageType)
}

func (page *page) getValueFromRawData(columnNameIndexMap map[string]int, schemaElements []*parquet.SchemaElement) (err error) {
	pageType := page.Header.GetType()
	switch pageType {
	case parquet.PageType_DICTIONARY_PAGE:
		bytesReader := bytes.NewReader(page.RawData)
		var values interface{}
		values, err = readValues(bytesReader, page.DataType,
			uint64(page.Header.DictionaryPageHeader.GetNumValues()), 0)
		if err != nil {
			return err
		}

		page.DataTable.Values = getTableValues(values, page.DataType)
		return nil

	case parquet.PageType_DATA_PAGE_V2:
		if page.RawData, err = compressionCodec(page.CompressType).uncompress(page.RawData); err != nil {
			return err
		}
		fallthrough
	case parquet.PageType_DATA_PAGE:
		encodingType := page.Header.DataPageHeader.GetEncoding()
		bytesReader := bytes.NewReader(page.RawData)

		var numNulls uint64
		for i := 0; i < len(page.DataTable.DefinitionLevels); i++ {
			if page.DataTable.DefinitionLevels[i] != page.DataTable.MaxDefinitionLevel {
				numNulls++
			}
		}

		name := strings.Join(page.DataTable.Path, ".")
		var convertedType parquet.ConvertedType = -1

		if schemaElements[columnNameIndexMap[name]].IsSetConvertedType() {
			convertedType = schemaElements[columnNameIndexMap[name]].GetConvertedType()
		}

		values, _, err := readDataPageValues(bytesReader, encodingType, page.DataType,
			convertedType, uint64(len(page.DataTable.DefinitionLevels))-numNulls,
			uint64(schemaElements[columnNameIndexMap[name]].GetTypeLength()))
		if err != nil {
			return err
		}

		tableValues := getTableValues(values, page.DataType)

		j := 0
		for i := 0; i < len(page.DataTable.DefinitionLevels); i++ {
			if page.DataTable.DefinitionLevels[i] == page.DataTable.MaxDefinitionLevel {
				page.DataTable.Values[i] = tableValues[j]
				j++
			}
		}

		page.RawData = []byte{}
		return nil
	}

	return fmt.Errorf("unsupported page type %v", pageType)
}
