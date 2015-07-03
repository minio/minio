// Package crc32c provides wrapper around Intel's fast CRC32C with PCLMULQDQ instructions.
// The white papers on CRC32C calculations with PCLMULQDQ instruction can be downloaded from:
//
// http://www.intel.com/content/dam/www/public/us/en/documents/white-papers/crc-iscsi-polynomial-crc32-instruction-paper.pdf
// http://www.intel.com/content/dam/www/public/us/en/documents/white-papers/fast-crc-computation-paper.pdf
//
// Example
//
// crc32c.Crc32c(value) - value can be any []byte, return value is uint32 value
package crc32c
