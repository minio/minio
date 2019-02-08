package compressutil

import (
	"bytes"
	"compress/gzip"
	"compress/lzw"
	"fmt"
	"io"

	"github.com/golang/snappy"
	"github.com/hashicorp/errwrap"
	"github.com/pierrec/lz4"
)

const (
	// A byte value used as a canary prefix for the compressed information
	// which is used to distinguish if a JSON input is compressed or not.
	// The value of this constant should not be a first character of any
	// valid JSON string.

	CompressionTypeGzip        = "gzip"
	CompressionCanaryGzip byte = 'G'

	CompressionTypeLZW        = "lzw"
	CompressionCanaryLZW byte = 'L'

	CompressionTypeSnappy        = "snappy"
	CompressionCanarySnappy byte = 'S'

	CompressionTypeLZ4        = "lz4"
	CompressionCanaryLZ4 byte = '4'
)

// SnappyReadCloser embeds the snappy reader which implements the io.Reader
// interface. The decompress procedure in this utility expects an
// io.ReadCloser. This type implements the io.Closer interface to retain the
// generic way of decompression.
type CompressUtilReadCloser struct {
	io.Reader
}

// Close is a noop method implemented only to satisfy the io.Closer interface
func (c *CompressUtilReadCloser) Close() error {
	return nil
}

// CompressionConfig is used to select a compression type to be performed by
// Compress and Decompress utilities.
// Supported types are:
// * CompressionTypeLZW
// * CompressionTypeGzip
// * CompressionTypeSnappy
// * CompressionTypeLZ4
//
// When using CompressionTypeGzip, the compression levels can also be chosen:
// * gzip.DefaultCompression
// * gzip.BestSpeed
// * gzip.BestCompression
type CompressionConfig struct {
	// Type of the compression algorithm to be used
	Type string

	// When using Gzip format, the compression level to employ
	GzipCompressionLevel int
}

// Compress places the canary byte in a buffer and uses the same buffer to fill
// in the compressed information of the given input. The configuration supports
// two type of compression: LZW and Gzip. When using Gzip compression format,
// if GzipCompressionLevel is not specified, the 'gzip.DefaultCompression' will
// be assumed.
func Compress(data []byte, config *CompressionConfig) ([]byte, error) {
	var buf bytes.Buffer
	var writer io.WriteCloser
	var err error

	if config == nil {
		return nil, fmt.Errorf("config is nil")
	}

	// Write the canary into the buffer and create writer to compress the
	// input data based on the configured type
	switch config.Type {
	case CompressionTypeLZW:
		buf.Write([]byte{CompressionCanaryLZW})
		writer = lzw.NewWriter(&buf, lzw.LSB, 8)

	case CompressionTypeGzip:
		buf.Write([]byte{CompressionCanaryGzip})

		switch {
		case config.GzipCompressionLevel == gzip.BestCompression,
			config.GzipCompressionLevel == gzip.BestSpeed,
			config.GzipCompressionLevel == gzip.DefaultCompression:
			// These are valid compression levels
		default:
			// If compression level is set to NoCompression or to
			// any invalid value, fallback to Defaultcompression
			config.GzipCompressionLevel = gzip.DefaultCompression
		}
		writer, err = gzip.NewWriterLevel(&buf, config.GzipCompressionLevel)

	case CompressionTypeSnappy:
		buf.Write([]byte{CompressionCanarySnappy})
		writer = snappy.NewBufferedWriter(&buf)

	case CompressionTypeLZ4:
		buf.Write([]byte{CompressionCanaryLZ4})
		writer = lz4.NewWriter(&buf)

	default:
		return nil, fmt.Errorf("unsupported compression type")
	}

	if err != nil {
		return nil, errwrap.Wrapf("failed to create a compression writer: {{err}}", err)
	}

	if writer == nil {
		return nil, fmt.Errorf("failed to create a compression writer")
	}

	// Compress the input and place it in the same buffer containing the
	// canary byte.
	if _, err = writer.Write(data); err != nil {
		return nil, errwrap.Wrapf("failed to compress input data: err: {{err}}", err)
	}

	// Close the io.WriteCloser
	if err = writer.Close(); err != nil {
		return nil, err
	}

	// Return the compressed bytes with canary byte at the start
	return buf.Bytes(), nil
}

// Decompress checks if the first byte in the input matches the canary byte.
// If the first byte is a canary byte, then the input past the canary byte
// will be decompressed using the method specified in the given configuration.
// If the first byte isn't a canary byte, then the utility returns a boolean
// value indicating that the input was not compressed.
func Decompress(data []byte) ([]byte, bool, error) {
	var err error
	var reader io.ReadCloser
	if data == nil || len(data) == 0 {
		return nil, false, fmt.Errorf("'data' being decompressed is empty")
	}

	canary := data[0]
	cData := data[1:]

	switch canary {
	// If the first byte matches the canary byte, remove the canary
	// byte and try to decompress the data that is after the canary.
	case CompressionCanaryGzip:
		if len(data) < 2 {
			return nil, false, fmt.Errorf("invalid 'data' after the canary")
		}
		reader, err = gzip.NewReader(bytes.NewReader(cData))

	case CompressionCanaryLZW:
		if len(data) < 2 {
			return nil, false, fmt.Errorf("invalid 'data' after the canary")
		}
		reader = lzw.NewReader(bytes.NewReader(cData), lzw.LSB, 8)

	case CompressionCanarySnappy:
		if len(data) < 2 {
			return nil, false, fmt.Errorf("invalid 'data' after the canary")
		}
		reader = &CompressUtilReadCloser{
			Reader: snappy.NewReader(bytes.NewReader(cData)),
		}

	case CompressionCanaryLZ4:
		if len(data) < 2 {
			return nil, false, fmt.Errorf("invalid 'data' after the canary")
		}
		reader = &CompressUtilReadCloser{
			Reader: lz4.NewReader(bytes.NewReader(cData)),
		}

	default:
		// If the first byte doesn't match the canary byte, it means
		// that the content was not compressed at all. Indicate the
		// caller that the input was not compressed.
		return nil, true, nil
	}
	if err != nil {
		return nil, false, errwrap.Wrapf("failed to create a compression reader: {{err}}", err)
	}
	if reader == nil {
		return nil, false, fmt.Errorf("failed to create a compression reader")
	}

	// Close the io.ReadCloser
	defer reader.Close()

	// Read all the compressed data into a buffer
	var buf bytes.Buffer
	if _, err = io.Copy(&buf, reader); err != nil {
		return nil, false, err
	}

	return buf.Bytes(), false, nil
}
