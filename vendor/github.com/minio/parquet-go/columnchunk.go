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

package parquet

import "github.com/minio/parquet-go/gen-go/parquet"

type columnChunk struct {
	Pages       []*page
	chunkHeader *parquet.ColumnChunk
}

func pagesToColumnChunk(pages []*page) *columnChunk {
	var numValues, totalUncompressedSize, totalCompressedSize int64
	minVal := pages[0].MinVal
	maxVal := pages[0].MaxVal
	parquetType, convertedType := pages[0].DataTable.Type, pages[0].DataTable.ConvertedType

	for i := 0; i < len(pages); i++ {
		if pages[i].Header.DataPageHeader != nil {
			numValues += int64(pages[i].Header.DataPageHeader.NumValues)
		} else {
			numValues += int64(pages[i].Header.DataPageHeaderV2.NumValues)
		}
		totalUncompressedSize += int64(pages[i].Header.UncompressedPageSize) + int64(len(pages[i].RawData)) - int64(pages[i].Header.CompressedPageSize)
		totalCompressedSize += int64(len(pages[i].RawData))
		minVal = min(minVal, pages[i].MinVal, &parquetType, &convertedType)
		maxVal = max(maxVal, pages[i].MaxVal, &parquetType, &convertedType)
	}

	metaData := parquet.NewColumnMetaData()
	metaData.Type = pages[0].DataType
	metaData.Encodings = []parquet.Encoding{
		parquet.Encoding_RLE,
		parquet.Encoding_BIT_PACKED,
		parquet.Encoding_PLAIN,
		// parquet.Encoding_DELTA_BINARY_PACKED,
	}
	metaData.Codec = pages[0].CompressType
	metaData.NumValues = numValues
	metaData.TotalCompressedSize = totalCompressedSize
	metaData.TotalUncompressedSize = totalUncompressedSize
	metaData.PathInSchema = pages[0].Path
	metaData.Statistics = parquet.NewStatistics()
	if maxVal != nil && minVal != nil {
		tmpBufMin := valueToBytes(minVal, parquetType)
		tmpBufMax := valueToBytes(maxVal, parquetType)

		if convertedType == parquet.ConvertedType_UTF8 || convertedType == parquet.ConvertedType_DECIMAL {
			tmpBufMin = tmpBufMin[4:]
			tmpBufMax = tmpBufMax[4:]
		}

		metaData.Statistics.Min = tmpBufMin
		metaData.Statistics.Max = tmpBufMax
	}

	chunk := new(columnChunk)
	chunk.Pages = pages
	chunk.chunkHeader = parquet.NewColumnChunk()
	chunk.chunkHeader.MetaData = metaData
	return chunk
}

func pagesToDictColumnChunk(pages []*page) *columnChunk {
	if len(pages) < 2 {
		return nil
	}

	var numValues, totalUncompressedSize, totalCompressedSize int64
	minVal := pages[1].MinVal
	maxVal := pages[1].MaxVal
	parquetType, convertedType := pages[1].DataTable.Type, pages[1].DataTable.ConvertedType

	for i := 0; i < len(pages); i++ {
		if pages[i].Header.DataPageHeader != nil {
			numValues += int64(pages[i].Header.DataPageHeader.NumValues)
		} else {
			numValues += int64(pages[i].Header.DataPageHeaderV2.NumValues)
		}
		totalUncompressedSize += int64(pages[i].Header.UncompressedPageSize) + int64(len(pages[i].RawData)) - int64(pages[i].Header.CompressedPageSize)
		totalCompressedSize += int64(len(pages[i].RawData))
		if i > 0 {
			minVal = min(minVal, pages[i].MinVal, &parquetType, &convertedType)
			maxVal = max(maxVal, pages[i].MaxVal, &parquetType, &convertedType)
		}
	}

	metaData := parquet.NewColumnMetaData()
	metaData.Type = pages[1].DataType
	metaData.Encodings = []parquet.Encoding{
		parquet.Encoding_RLE,
		parquet.Encoding_BIT_PACKED,
		parquet.Encoding_PLAIN,
		parquet.Encoding_PLAIN_DICTIONARY,
	}
	metaData.Codec = pages[1].CompressType
	metaData.NumValues = numValues
	metaData.TotalCompressedSize = totalCompressedSize
	metaData.TotalUncompressedSize = totalUncompressedSize
	metaData.PathInSchema = pages[1].Path
	metaData.Statistics = parquet.NewStatistics()
	if maxVal != nil && minVal != nil {
		tmpBufMin := valueToBytes(minVal, parquetType)
		tmpBufMax := valueToBytes(maxVal, parquetType)

		if convertedType == parquet.ConvertedType_UTF8 || convertedType == parquet.ConvertedType_DECIMAL {
			tmpBufMin = tmpBufMin[4:]
			tmpBufMax = tmpBufMax[4:]
		}

		metaData.Statistics.Min = tmpBufMin
		metaData.Statistics.Max = tmpBufMax
	}

	chunk := new(columnChunk)
	chunk.Pages = pages
	chunk.chunkHeader = parquet.NewColumnChunk()
	chunk.chunkHeader.MetaData = metaData
	return chunk
}

func decodeDictColumnChunk(chunk *columnChunk) {
	dictPage := chunk.Pages[0]
	numPages := len(chunk.Pages)
	for i := 1; i < numPages; i++ {
		numValues := len(chunk.Pages[i].DataTable.Values)
		for j := 0; j < numValues; j++ {
			if chunk.Pages[i].DataTable.Values[j] != nil {
				index := chunk.Pages[i].DataTable.Values[j].(int64)
				chunk.Pages[i].DataTable.Values[j] = dictPage.DataTable.Values[index]
			}
		}
	}
	chunk.Pages = chunk.Pages[1:] // delete the head dict page
}
