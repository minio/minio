// Package erasure is a Go wrapper for the Intel Intelligent Storage
// Acceleration Library (Intel ISA-L).  Intel ISA-L is a CPU optimized
// implementation of erasure code.
//
// For more information on Intel ISA-L, please visit:
// https://01.org/intel%C2%AE-storage-acceleration-library-open-source-version
//
// Usage
//
// TODO: Explain Encode and Decode inputs and outputs
//
// TODO: Explain matrix size and how it corresponds to protection level
//
// Encoding data may be performed in 3 steps.
//
//  1. Create a parse set of encoder parameters
//  2. Create a new encoder
//  3. Encode data
//
// Decoding data is also performed in 3 steps.
//
//  1. Create a parse set of encoder parameters for validation
//  2. Create a new encoder
//  3. Decode data
//
// Encoder parameters contain four configurable elements:
//  ParseEncoderParams(k, m, technique int) (EncoderParams, error)
//  k - Number of rows in matrix
//  m - Number of colums in matrix
//  technique - Matrix type, can be either CAUCHY (recommended) or VANDERMONDE
//  constraints: k + m < Galois Field (2^8)
//
// Example
//
// Creating and using an encoder
//  var bytes []byte
//  params := erasure.ParseEncoderParams(10, 5, erasure.CAUCHY)
//  encoder := erasure.NewEncoder(params)
//  encodedData, length := encoder.Encode(bytes)
//
// Creating and using a decoder
//  var encodedData [][]byte
//  var length int
//  params := erasure.ParseEncoderParams(10, 5, erasure.CAUCHY)
//  encoder := erasure.NewEncoder(params)
//  originalData, err := encoder.Decode(encodedData, length)
package erasure
