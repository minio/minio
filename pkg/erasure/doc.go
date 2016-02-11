// Package erasure is a Go wrapper for the Intel Intelligent Storage
// Acceleration Library (Intel ISA-L).  Intel ISA-L is a CPU optimized
// implementation of erasure coding algorithms.
//
// For more information on Intel ISA-L, please visit:
// https://01.org/intel%C2%AE-storage-acceleration-library-open-source-version
//
// Usage:
//
// Encode encodes a block of data. The input is the original data. The output
// is a 2 tuple containing (k + m) chunks of erasure encoded data and the
// length of the original object.
//
// Decode decodes 2 tuple data containing (k + m) chunks back into its original form.
// Additionally original block length should also be provided as input.
//
// Decoded data is exactly similar in length and content as the original data.
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
// Erasure parameters contain three configurable elements:
//  ValidateParams(k, m, technique int) (ErasureParams, error)
//  k - Number of rows in matrix
//  m - Number of colums in matrix
//  technique - Matrix type, can be either Cauchy (recommended) or Vandermonde
//  constraints: k + m < Galois Field (2^8)
//
// Choosing right parity and matrix technique is left for application to decide.
//
// But here are the few points to keep in mind
//
//  Matrix Type:
//     - Vandermonde is most commonly used method for choosing coefficients in erasure
//       encoding but does not guarantee invertable for every sub matrix.
//     - Whereas Cauchy is our recommended method for choosing coefficients in erasure coding.
//       Since any sub-matrix of a Cauchy matrix is invertable.
//
//  Total blocks:
//     - Data blocks and Parity blocks should not be greater than 'Galois Field' (2^8)
//
// Example
//
// Creating and using an encoder
//  var bytes []byte
//  params := erasure.ValidateParams(10, 5)
//  encoder := erasure.NewErasure(params)
//  encodedData, length := encoder.Encode(bytes)
//
// Creating and using a decoder
//  var encodedData [][]byte
//  var length int
//  params := erasure.ValidateParams(10, 5)
//  encoder := erasure.NewErasure(params)
//  originalData, err := encoder.Decode(encodedData, length)
//
package erasure
