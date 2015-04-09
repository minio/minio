/**********************************************************************
  Copyright(c) 2011-2015 Intel Corporation All rights reserved.

  Redistribution and use in source and binary forms, with or without
  modification, are permitted provided that the following conditions 
  are met:
    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in
      the documentation and/or other materials provided with the
      distribution.
    * Neither the name of Intel Corporation nor the names of its
      contributors may be used to endorse or promote products derived
      from this software without specific prior written permission.

  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
  "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
  LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
  A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
  OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
  SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
  LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
  DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
  THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
  OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
**********************************************************************/


#ifndef _ERASURE_CODE_H_
#define _ERASURE_CODE_H_

/**
 *  @file erasure_code.h
 *  @brief Interface to functions supporting erasure code encode and decode.
 *
 *  This file defines the interface to optimized functions used in erasure
 *  codes.  Encode and decode of erasures in GF(2^8) are made by calculating the
 *  dot product of the symbols (bytes in GF(2^8)) across a set of buffers and a
 *  set of coefficients.  Values for the coefficients are determined by the type
 *  of erasure code.  Using a general dot product means that any sequence of
 *  coefficients may be used including erasure codes based on random 
 *  coefficients.
 *  Multiple versions of dot product are supplied to calculate 1-6 output
 *  vectors in one pass.
 *  Base GF multiply and divide functions can be sped up by defining
 *  GF_LARGE_TABLES at the expense of memory size.
 *
 */

#include "gf_vect_mul.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Initialize tables for fast Erasure Code encode and decode.
 *
 * Generates the expanded tables needed for fast encode or decode for erasure
 * codes on blocks of data.  32bytes is generated for each input coefficient.
 *
 * @param k      The number of vector sources or rows in the generator matrix
 *               for coding.
 * @param rows   The number of output vectors to concurrently encode/decode.
 * @param a      Pointer to sets of arrays of input coefficients used to encode
 *               or decode data.
 * @param gftbls Pointer to start of space for concatenated output tables
 *               generated from input coefficients.  Must be of size 32*k*rows.
 * @returns none
 */

void ec_init_tables(int k, int rows, unsigned char* a, unsigned char* gftbls);

/**
 * @brief Generate or decode erasure codes on blocks of data, runs appropriate version.
 *
 * Given a list of source data blocks, generate one or multiple blocks of
 * encoded data as specified by a matrix of GF(2^8) coefficients. When given a
 * suitable set of coefficients, this function will perform the fast generation
 * or decoding of Reed-Solomon type erasure codes.
 * 
 * This function determines what instruction sets are enabled and
 * selects the appropriate version at runtime.
 * 
 * @param len    Length of each block of data (vector) of source or dest data.
 * @param k      The number of vector sources or rows in the generator matrix
 * 		 for coding.
 * @param rows   The number of output vectors to concurrently encode/decode.
 * @param gftbls Pointer to array of input tables generated from coding
 * 		 coefficients in ec_init_tables(). Must be of size 32*k*rows
 * @param data   Array of pointers to source input buffers.
 * @param coding Array of pointers to coded output buffers.
 * @returns none
 */

void ec_encode_data(int len, int k, int rows, unsigned char *gftbls, unsigned char **data,
		    unsigned char **coding);

/**
 * @brief Generate or decode erasure codes on blocks of data.
 *
 * Arch specific version of ec_encode_data() with same parameters.
 * @requires SSE4.1
 */
void ec_encode_data_sse(int len, int k, int rows, unsigned char *gftbls, unsigned char **data,
			unsigned char **coding);

/**
 * @brief Generate or decode erasure codes on blocks of data.
 *
 * Arch specific version of ec_encode_data() with same parameters.
 * @requires AVX
 */
void ec_encode_data_avx(int len, int k, int rows, unsigned char *gftbls, unsigned char **data,
			unsigned char **coding);

/**
 * @brief Generate or decode erasure codes on blocks of data.
 *
 * Arch specific version of ec_encode_data() with same parameters.
 * @requires AVX2
 */
void ec_encode_data_avx2(int len, int k, int rows, unsigned char *gftbls, unsigned char **data,
			 unsigned char **coding);

/**
 * @brief Generate or decode erasure codes on blocks of data, runs baseline version.
 *
 * Baseline version of ec_encode_data() with same parameters.
 */
void ec_encode_data_base(int len, int srcs, int dests, unsigned char *v, unsigned char **src,
			 unsigned char **dest);

/**
 * @brief Generate update for encode or decode of erasure codes from single source, runs appropriate version.
 *
 * Given one source data block, update one or multiple blocks of encoded data as
 * specified by a matrix of GF(2^8) coefficients. When given a suitable set of
 * coefficients, this function will perform the fast generation or decoding of
 * Reed-Solomon type erasure codes from one input source at a time.
 * 
 * This function determines what instruction sets are enabled and selects the
 * appropriate version at runtime.
 * 
 * @param len    Length of each block of data (vector) of source or dest data.
 * @param k      The number of vector sources or rows in the generator matrix
 * 		 for coding.
 * @param rows   The number of output vectors to concurrently encode/decode.
 * @param vec_i  The vector index corresponding to the single input source.
 * @param g_tbls Pointer to array of input tables generated from coding
 * 		 coefficients in ec_init_tables(). Must be of size 32*k*rows
 * @param data   Pointer to single input source used to update output parity.
 * @param coding Array of pointers to coded output buffers.
 * @returns none
 */
void ec_encode_data_update(int len, int k, int rows, int vec_i, unsigned char *g_tbls,
			   unsigned char *data, unsigned char **coding);

/**
 * @brief Generate update for encode or decode of erasure codes from single source.
 *
 * Arch specific version of ec_encode_data_update() with same parameters.
 * @requires SSE4.1
 */

void ec_encode_data_update_sse(int len, int k, int rows, int vec_i, unsigned char *g_tbls,
			       unsigned char *data, unsigned char **coding);

/**
 * @brief Generate update for encode or decode of erasure codes from single source.
 *
 * Arch specific version of ec_encode_data_update() with same parameters.
 * @requires AVX
 */

void ec_encode_data_update_avx(int len, int k, int rows, int vec_i, unsigned char *g_tbls,
			       unsigned char *data, unsigned char **coding);

/**
 * @brief Generate update for encode or decode of erasure codes from single source.
 *
 * Arch specific version of ec_encode_data_update() with same parameters.
 * @requires AVX2
 */

void ec_encode_data_update_avx2(int len, int k, int rows, int vec_i, unsigned char *g_tbls,
				unsigned char *data, unsigned char **coding);

/**
 * @brief Generate update for encode or decode of erasure codes from single source.
 *
 * Baseline version of ec_encode_data_update().
 */

void ec_encode_data_update_base(int len, int k, int rows, int vec_i, unsigned char *v, 
				unsigned char *data, unsigned char **dest);


/**
 * @brief GF(2^8) vector dot product.
 *
 * Does a GF(2^8) dot product across each byte of the input array and a constant
 * set of coefficients to produce each byte of the output. Can be used for
 * erasure coding encode and decode. Function requires pre-calculation of a
 * 32*vlen byte constant array based on the input coefficients.
 * @requires SSE4.1
 *
 * @param len    Length of each vector in bytes. Must be >= 16.
 * @param vlen   Number of vector sources.
 * @param gftbls Pointer to 32*vlen byte array of pre-calculated constants based
 *               on the array of input coefficients.
 * @param src    Array of pointers to source inputs.
 * @param dest   Pointer to destination data array.
 * @returns none
 */

void gf_vect_dot_prod_sse(int len, int vlen, unsigned char *gftbls, 
			unsigned char **src, unsigned char *dest);

/**
 * @brief GF(2^8) vector dot product.
 *
 * Does a GF(2^8) dot product across each byte of the input array and a constant
 * set of coefficients to produce each byte of the output. Can be used for
 * erasure coding encode and decode. Function requires pre-calculation of a
 * 32*vlen byte constant array based on the input coefficients.
 * @requires AVX
 *
 * @param len    Length of each vector in bytes. Must be >= 16.
 * @param vlen   Number of vector sources.
 * @param gftbls Pointer to 32*vlen byte array of pre-calculated constants based
 *               on the array of input coefficients.
 * @param src    Array of pointers to source inputs.
 * @param dest   Pointer to destination data array.
 * @returns none
 */

void gf_vect_dot_prod_avx(int len, int vlen, unsigned char *gftbls, 
			unsigned char **src, unsigned char *dest);

/**
 * @brief GF(2^8) vector dot product.
 *
 * Does a GF(2^8) dot product across each byte of the input array and a constant
 * set of coefficients to produce each byte of the output. Can be used for
 * erasure coding encode and decode. Function requires pre-calculation of a
 * 32*vlen byte constant array based on the input coefficients.
 * @requires AVX2
 *
 * @param len    Length of each vector in bytes. Must be >= 32.
 * @param vlen   Number of vector sources.
 * @param gftbls Pointer to 32*vlen byte array of pre-calculated constants based
 *               on the array of input coefficients.
 * @param src    Array of pointers to source inputs.
 * @param dest   Pointer to destination data array.
 * @returns none
 */

void gf_vect_dot_prod_avx2(int len, int vlen, unsigned char *gftbls, 
			unsigned char **src, unsigned char *dest);

/**
 * @brief GF(2^8) vector dot product with two outputs.
 *
 * Vector dot product optimized to calculate two ouputs at a time. Does two
 * GF(2^8) dot products across each byte of the input array and two constant
 * sets of coefficients to produce each byte of the outputs. Can be used for
 * erasure coding encode and decode. Function requires pre-calculation of a
 * 2*32*vlen byte constant array based on the two sets of input coefficients.
 * @requires SSE4.1
 *
 * @param len    Length of each vector in bytes. Must be >= 16.
 * @param vlen   Number of vector sources.
 * @param gftbls Pointer to 2*32*vlen byte array of pre-calculated constants
 *               based on the array of input coefficients.
 * @param src    Array of pointers to source inputs.
 * @param dest   Array of pointers to destination data buffers.  
 * @returns none
 */

void gf_2vect_dot_prod_sse(int len, int vlen, unsigned char *gftbls,
			unsigned char **src, unsigned char **dest);

/**
 * @brief GF(2^8) vector dot product with two outputs.
 *
 * Vector dot product optimized to calculate two ouputs at a time. Does two
 * GF(2^8) dot products across each byte of the input array and two constant
 * sets of coefficients to produce each byte of the outputs. Can be used for
 * erasure coding encode and decode. Function requires pre-calculation of a
 * 2*32*vlen byte constant array based on the two sets of input coefficients.
 * @requires AVX
 *
 * @param len    Length of each vector in bytes. Must be >= 16.
 * @param vlen   Number of vector sources.
 * @param gftbls Pointer to 2*32*vlen byte array of pre-calculated constants
 *               based on the array of input coefficients.
 * @param src    Array of pointers to source inputs.
 * @param dest   Array of pointers to destination data buffers.  
 * @returns none
 */

void gf_2vect_dot_prod_avx(int len, int vlen, unsigned char *gftbls,
			unsigned char **src, unsigned char **dest);

/**
 * @brief GF(2^8) vector dot product with two outputs.
 *
 * Vector dot product optimized to calculate two ouputs at a time. Does two
 * GF(2^8) dot products across each byte of the input array and two constant
 * sets of coefficients to produce each byte of the outputs. Can be used for
 * erasure coding encode and decode. Function requires pre-calculation of a
 * 2*32*vlen byte constant array based on the two sets of input coefficients.
 * @requires AVX2
 *
 * @param len    Length of each vector in bytes. Must be >= 32.
 * @param vlen   Number of vector sources.
 * @param gftbls Pointer to 2*32*vlen byte array of pre-calculated constants
 *               based on the array of input coefficients.
 * @param src    Array of pointers to source inputs.
 * @param dest   Array of pointers to destination data buffers.  
 * @returns none
 */

void gf_2vect_dot_prod_avx2(int len, int vlen, unsigned char *gftbls,
			unsigned char **src, unsigned char **dest);

/**
 * @brief GF(2^8) vector dot product with three outputs.
 *
 * Vector dot product optimized to calculate three ouputs at a time. Does three
 * GF(2^8) dot products across each byte of the input array and three constant
 * sets of coefficients to produce each byte of the outputs. Can be used for
 * erasure coding encode and decode. Function requires pre-calculation of a
 * 3*32*vlen byte constant array based on the three sets of input coefficients.
 * @requires SSE4.1
 *
 * @param len    Length of each vector in bytes. Must be >= 16.
 * @param vlen   Number of vector sources.
 * @param gftbls Pointer to 3*32*vlen byte array of pre-calculated constants
 *               based on the array of input coefficients.
 * @param src    Array of pointers to source inputs.
 * @param dest   Array of pointers to destination data buffers.  
 * @returns none
 */

void gf_3vect_dot_prod_sse(int len, int vlen, unsigned char *gftbls,
			unsigned char **src, unsigned char **dest);

/**
 * @brief GF(2^8) vector dot product with three outputs.
 *
 * Vector dot product optimized to calculate three ouputs at a time. Does three
 * GF(2^8) dot products across each byte of the input array and three constant
 * sets of coefficients to produce each byte of the outputs. Can be used for
 * erasure coding encode and decode. Function requires pre-calculation of a
 * 3*32*vlen byte constant array based on the three sets of input coefficients.
 * @requires AVX
 *
 * @param len    Length of each vector in bytes. Must be >= 16.
 * @param vlen   Number of vector sources.
 * @param gftbls Pointer to 3*32*vlen byte array of pre-calculated constants
 *               based on the array of input coefficients.
 * @param src    Array of pointers to source inputs.
 * @param dest   Array of pointers to destination data buffers.  
 * @returns none
 */

void gf_3vect_dot_prod_avx(int len, int vlen, unsigned char *gftbls,
			unsigned char **src, unsigned char **dest);

/**
 * @brief GF(2^8) vector dot product with three outputs.
 *
 * Vector dot product optimized to calculate three ouputs at a time. Does three
 * GF(2^8) dot products across each byte of the input array and three constant
 * sets of coefficients to produce each byte of the outputs. Can be used for
 * erasure coding encode and decode. Function requires pre-calculation of a
 * 3*32*vlen byte constant array based on the three sets of input coefficients.
 * @requires AVX2
 *
 * @param len    Length of each vector in bytes. Must be >= 32.
 * @param vlen   Number of vector sources.
 * @param gftbls Pointer to 3*32*vlen byte array of pre-calculated constants
 *               based on the array of input coefficients.
 * @param src    Array of pointers to source inputs.
 * @param dest   Array of pointers to destination data buffers.  
 * @returns none
 */

void gf_3vect_dot_prod_avx2(int len, int vlen, unsigned char *gftbls,
			unsigned char **src, unsigned char **dest);

/**
 * @brief GF(2^8) vector dot product with four outputs.
 *
 * Vector dot product optimized to calculate four ouputs at a time. Does four
 * GF(2^8) dot products across each byte of the input array and four constant
 * sets of coefficients to produce each byte of the outputs. Can be used for
 * erasure coding encode and decode. Function requires pre-calculation of a
 * 4*32*vlen byte constant array based on the four sets of input coefficients.
 * @requires SSE4.1
 *
 * @param len    Length of each vector in bytes. Must be >= 16.
 * @param vlen   Number of vector sources.
 * @param gftbls Pointer to 4*32*vlen byte array of pre-calculated constants
 *               based on the array of input coefficients.
 * @param src    Array of pointers to source inputs.
 * @param dest   Array of pointers to destination data buffers.
 * @returns none
 */

void gf_4vect_dot_prod_sse(int len, int vlen, unsigned char *gftbls,
			unsigned char **src, unsigned char **dest);

/**
 * @brief GF(2^8) vector dot product with four outputs.
 *
 * Vector dot product optimized to calculate four ouputs at a time. Does four
 * GF(2^8) dot products across each byte of the input array and four constant
 * sets of coefficients to produce each byte of the outputs. Can be used for
 * erasure coding encode and decode. Function requires pre-calculation of a
 * 4*32*vlen byte constant array based on the four sets of input coefficients.
 * @requires AVX
 *
 * @param len    Length of each vector in bytes. Must be >= 16.
 * @param vlen   Number of vector sources.
 * @param gftbls Pointer to 4*32*vlen byte array of pre-calculated constants
 *               based on the array of input coefficients.
 * @param src    Array of pointers to source inputs.
 * @param dest   Array of pointers to destination data buffers.
 * @returns none
 */

void gf_4vect_dot_prod_avx(int len, int vlen, unsigned char *gftbls,
			unsigned char **src, unsigned char **dest);

/**
 * @brief GF(2^8) vector dot product with four outputs.
 *
 * Vector dot product optimized to calculate four ouputs at a time. Does four
 * GF(2^8) dot products across each byte of the input array and four constant
 * sets of coefficients to produce each byte of the outputs. Can be used for
 * erasure coding encode and decode. Function requires pre-calculation of a
 * 4*32*vlen byte constant array based on the four sets of input coefficients.
 * @requires AVX2
 *
 * @param len    Length of each vector in bytes. Must be >= 32.
 * @param vlen   Number of vector sources.
 * @param gftbls Pointer to 4*32*vlen byte array of pre-calculated constants
 *               based on the array of input coefficients.
 * @param src    Array of pointers to source inputs.
 * @param dest   Array of pointers to destination data buffers.
 * @returns none
 */

void gf_4vect_dot_prod_avx2(int len, int vlen, unsigned char *gftbls,
			unsigned char **src, unsigned char **dest);

/**
 * @brief GF(2^8) vector dot product with five outputs.
 *
 * Vector dot product optimized to calculate five ouputs at a time. Does five
 * GF(2^8) dot products across each byte of the input array and five constant
 * sets of coefficients to produce each byte of the outputs. Can be used for
 * erasure coding encode and decode. Function requires pre-calculation of a
 * 5*32*vlen byte constant array based on the five sets of input coefficients.
 * @requires SSE4.1
 *
 * @param len    Length of each vector in bytes. Must >= 16.
 * @param vlen   Number of vector sources.
 * @param gftbls Pointer to 5*32*vlen byte array of pre-calculated constants
 *               based on the array of input coefficients.
 * @param src    Array of pointers to source inputs.
 * @param dest   Array of pointers to destination data buffers.  
 * @returns none
 */

void gf_5vect_dot_prod_sse(int len, int vlen, unsigned char *gftbls,
			unsigned char **src, unsigned char **dest);

/**
 * @brief GF(2^8) vector dot product with five outputs.
 *
 * Vector dot product optimized to calculate five ouputs at a time. Does five
 * GF(2^8) dot products across each byte of the input array and five constant
 * sets of coefficients to produce each byte of the outputs. Can be used for
 * erasure coding encode and decode. Function requires pre-calculation of a
 * 5*32*vlen byte constant array based on the five sets of input coefficients.
 * @requires AVX
 *
 * @param len    Length of each vector in bytes. Must >= 16.
 * @param vlen   Number of vector sources.
 * @param gftbls Pointer to 5*32*vlen byte array of pre-calculated constants
 *               based on the array of input coefficients.
 * @param src    Array of pointers to source inputs.
 * @param dest   Array of pointers to destination data buffers.  
 * @returns none
 */

void gf_5vect_dot_prod_avx(int len, int vlen, unsigned char *gftbls,
			unsigned char **src, unsigned char **dest);

/**
 * @brief GF(2^8) vector dot product with five outputs.
 *
 * Vector dot product optimized to calculate five ouputs at a time. Does five
 * GF(2^8) dot products across each byte of the input array and five constant
 * sets of coefficients to produce each byte of the outputs. Can be used for
 * erasure coding encode and decode. Function requires pre-calculation of a
 * 5*32*vlen byte constant array based on the five sets of input coefficients.
 * @requires AVX2
 *
 * @param len    Length of each vector in bytes. Must >= 32.
 * @param vlen   Number of vector sources.
 * @param gftbls Pointer to 5*32*vlen byte array of pre-calculated constants
 *               based on the array of input coefficients.
 * @param src    Array of pointers to source inputs.
 * @param dest   Array of pointers to destination data buffers.  
 * @returns none
 */

void gf_5vect_dot_prod_avx2(int len, int vlen, unsigned char *gftbls,
			unsigned char **src, unsigned char **dest);

/**
 * @brief GF(2^8) vector dot product with six outputs.
 *
 * Vector dot product optimized to calculate six ouputs at a time. Does six
 * GF(2^8) dot products across each byte of the input array and six constant
 * sets of coefficients to produce each byte of the outputs. Can be used for
 * erasure coding encode and decode. Function requires pre-calculation of a
 * 6*32*vlen byte constant array based on the six sets of input coefficients.
 * @requires SSE4.1
 *
 * @param len    Length of each vector in bytes. Must be >= 16.
 * @param vlen   Number of vector sources.
 * @param gftbls Pointer to 6*32*vlen byte array of pre-calculated constants
 *               based on the array of input coefficients.
 * @param src    Array of pointers to source inputs.
 * @param dest   Array of pointers to destination data buffers.  
 * @returns none
 */

void gf_6vect_dot_prod_sse(int len, int vlen, unsigned char *gftbls,
			unsigned char **src, unsigned char **dest);

/**
 * @brief GF(2^8) vector dot product with six outputs.
 *
 * Vector dot product optimized to calculate six ouputs at a time. Does six
 * GF(2^8) dot products across each byte of the input array and six constant
 * sets of coefficients to produce each byte of the outputs. Can be used for
 * erasure coding encode and decode. Function requires pre-calculation of a
 * 6*32*vlen byte constant array based on the six sets of input coefficients.
 * @requires AVX
 *
 * @param len    Length of each vector in bytes. Must be >= 16.
 * @param vlen   Number of vector sources.
 * @param gftbls Pointer to 6*32*vlen byte array of pre-calculated constants
 *               based on the array of input coefficients.
 * @param src    Array of pointers to source inputs.
 * @param dest   Array of pointers to destination data buffers.  
 * @returns none
 */

void gf_6vect_dot_prod_avx(int len, int vlen, unsigned char *gftbls,
			unsigned char **src, unsigned char **dest);

/**
 * @brief GF(2^8) vector dot product with six outputs.
 *
 * Vector dot product optimized to calculate six ouputs at a time. Does six
 * GF(2^8) dot products across each byte of the input array and six constant
 * sets of coefficients to produce each byte of the outputs. Can be used for
 * erasure coding encode and decode. Function requires pre-calculation of a
 * 6*32*vlen byte constant array based on the six sets of input coefficients.
 * @requires AVX2
 *
 * @param len    Length of each vector in bytes. Must be >= 32.
 * @param vlen   Number of vector sources.
 * @param gftbls Pointer to 6*32*vlen byte array of pre-calculated constants
 *               based on the array of input coefficients.
 * @param src    Array of pointers to source inputs.
 * @param dest   Array of pointers to destination data buffers.  
 * @returns none
 */

void gf_6vect_dot_prod_avx2(int len, int vlen, unsigned char *gftbls,
			unsigned char **src, unsigned char **dest);

/**
 * @brief GF(2^8) vector dot product, runs baseline version.
 * 
 * Does a GF(2^8) dot product across each byte of the input array and a constant
 * set of coefficients to produce each byte of the output. Can be used for
 * erasure coding encode and decode. Function requires pre-calculation of a
 * 32*vlen byte constant array based on the input coefficients.
 * 
 * @param len    Length of each vector in bytes. Must be >= 16.
 * @param vlen   Number of vector sources.
 * @param gftbls Pointer to 32*vlen byte array of pre-calculated constants based
 *               on the array of input coefficients. Only elements 32*CONST*j + 1 
 *               of this array are used, where j = (0, 1, 2...) and CONST is the
 *               number of elements in the array of input coefficients. The 
 *               elements used correspond to the original input coefficients.		
 * @param src    Array of pointers to source inputs.
 * @param dest   Pointer to destination data array.
 * @returns none
 */

void gf_vect_dot_prod_base(int len, int vlen, unsigned char *gftbls,
                        unsigned char **src, unsigned char *dest);

/**
 * @brief GF(2^8) vector dot product, runs appropriate version.
 *
 * Does a GF(2^8) dot product across each byte of the input array and a constant
 * set of coefficients to produce each byte of the output. Can be used for
 * erasure coding encode and decode. Function requires pre-calculation of a
 * 32*vlen byte constant array based on the input coefficients.
 * 
 * This function determines what instruction sets are enabled and
 * selects the appropriate version at runtime.
 *
 * @param len    Length of each vector in bytes. Must be >= 32.
 * @param vlen   Number of vector sources.
 * @param gftbls Pointer to 32*vlen byte array of pre-calculated constants based
 *               on the array of input coefficients.
 * @param src    Array of pointers to source inputs.
 * @param dest   Pointer to destination data array.
 * @returns none
 */

void gf_vect_dot_prod(int len, int vlen, unsigned char *gftbls,
                        unsigned char **src, unsigned char *dest);


/**
 * @brief GF(2^8) vector multiply accumulate, runs appropriate version.
 *
 * Does a GF(2^8) multiply across each byte of input source with expanded
 * constant and add to destination array. Can be used for erasure coding encode
 * and decode update when only one source is available at a time. Function
 * requires pre-calculation of a 32*vec byte constant array based on the input
 * coefficients.
 *
 * This function determines what instruction sets are enabled and selects the
 * appropriate version at runtime.
 *
 * @param len    Length of each vector in bytes. Must be >= 32.
 * @param vec    The number of vector sources or rows in the generator matrix
 * 		 for coding.
 * @param vec_i  The vector index corresponding to the single input source.
 * @param gftbls Pointer to array of input tables generated from coding
 * 		 coefficients in ec_init_tables(). Must be of size 32*vec.
 * @param src    Array of pointers to source inputs.
 * @param dest   Pointer to destination data array.
 * @returns none
 */

void gf_vect_mad(int len, int vec, int vec_i, unsigned char *gftbls, unsigned char *src,
		 unsigned char *dest);

/**
 * @brief GF(2^8) vector multiply accumulate, arch specific version.
 *
 * Arch specific version of gf_vect_mad() with same parameters.
 * @requires SSE4.1
 */

void gf_vect_mad_sse(int len, int vec, int vec_i, unsigned char *gftbls, unsigned char *src,
		     unsigned char *dest);
/**
 * @brief GF(2^8) vector multiply accumulate, arch specific version.
 *
 * Arch specific version of gf_vect_mad() with same parameters.
 * @requires AVX
 */

void gf_vect_mad_avx(int len, int vec, int vec_i, unsigned char *gftbls, unsigned char *src,
		     unsigned char *dest);

/**
 * @brief GF(2^8) vector multiply accumulate, arch specific version.
 *
 * Arch specific version of gf_vect_mad() with same parameters.
 * @requires AVX2
 */

void gf_vect_mad_avx2(int len, int vec, int vec_i, unsigned char *gftbls, unsigned char *src,
		      unsigned char *dest);

/**
 * @brief GF(2^8) vector multiply accumulate, baseline version.
 *
 * Baseline version of gf_vect_mad() with same parameters.
 */

void gf_vect_mad_base(int len, int vec, int vec_i, unsigned char *v, unsigned char *src,
		      unsigned char *dest);

/**
 * @brief GF(2^8) vector multiply with 2 accumulate.  SSE version.
 *
 * Does a GF(2^8) multiply across each byte of input source with expanded
 * constants and add to destination arrays. Can be used for erasure coding
 * encode and decode update when only one source is available at a
 * time. Function requires pre-calculation of a 32*vec byte constant array based
 * on the input coefficients.
 * @requires SSE4.1
 *
 * @param len    Length of each vector in bytes. Must be >= 32.
 * @param vec    The number of vector sources or rows in the generator matrix
 * 		 for coding.
 * @param vec_i  The vector index corresponding to the single input source.
 * @param gftbls Pointer to array of input tables generated from coding
 * 		 coefficients in ec_init_tables(). Must be of size 32*vec.
 * @param src    Pointer to source input array.
 * @param dest   Array of pointers to destination input/outputs.
 * @returns none
 */

void gf_2vect_mad_sse(int len, int vec, int vec_i, unsigned char *gftbls, unsigned char *src,
		      unsigned char **dest);

/**
 * @brief GF(2^8) vector multiply with 2 accumulate. AVX version of gf_2vect_mad_sse().
 * @requires AVX
 */
void gf_2vect_mad_avx(int len, int vec, int vec_i, unsigned char *gftbls, unsigned char *src,
		      unsigned char **dest);
/**
 * @brief GF(2^8) vector multiply with 2 accumulate. AVX2 version of gf_2vect_mad_sse().
 * @requires AVX2
 */
void gf_2vect_mad_avx2(int len, int vec, int vec_i, unsigned char *gftbls, unsigned char *src,
		       unsigned char **dest);

/**
 * @brief GF(2^8) vector multiply with 3 accumulate. SSE version.
 *
 * Does a GF(2^8) multiply across each byte of input source with expanded
 * constants and add to destination arrays. Can be used for erasure coding
 * encode and decode update when only one source is available at a
 * time. Function requires pre-calculation of a 32*vec byte constant array based
 * on the input coefficients.
 * @requires SSE4.1
 *
 * @param len    Length of each vector in bytes. Must be >= 32.
 * @param vec    The number of vector sources or rows in the generator matrix
 * 		 for coding.
 * @param vec_i  The vector index corresponding to the single input source.
 * @param gftbls Pointer to array of input tables generated from coding
 * 		 coefficients in ec_init_tables(). Must be of size 32*vec.
 * @param src    Pointer to source input array.
 * @param dest   Array of pointers to destination input/outputs.
 * @returns none
 */

void gf_3vect_mad_sse(int len, int vec, int vec_i, unsigned char *gftbls, unsigned char *src,
		      unsigned char **dest);

/**
 * @brief GF(2^8) vector multiply with 3 accumulate. AVX version of gf_3vect_mad_sse().
 * @requires AVX
 */
void gf_3vect_mad_avx(int len, int vec, int vec_i, unsigned char *gftbls, unsigned char *src,
		      unsigned char **dest);

/**
 * @brief GF(2^8) vector multiply with 3 accumulate. AVX2 version of gf_3vect_mad_sse().
 * @requires AVX2
 */
void gf_3vect_mad_avx2(int len, int vec, int vec_i, unsigned char *gftbls, unsigned char *src,
		       unsigned char **dest);

/**
 * @brief GF(2^8) vector multiply with 4 accumulate. SSE version.
 *
 * Does a GF(2^8) multiply across each byte of input source with expanded
 * constants and add to destination arrays. Can be used for erasure coding
 * encode and decode update when only one source is available at a
 * time. Function requires pre-calculation of a 32*vec byte constant array based
 * on the input coefficients.
 * @requires SSE4.1
 *
 * @param len    Length of each vector in bytes. Must be >= 32.
 * @param vec    The number of vector sources or rows in the generator matrix
 * 		 for coding.
 * @param vec_i  The vector index corresponding to the single input source.
 * @param gftbls Pointer to array of input tables generated from coding
 * 		 coefficients in ec_init_tables(). Must be of size 32*vec.
 * @param src    Pointer to source input array.
 * @param dest   Array of pointers to destination input/outputs.
 * @returns none
 */

void gf_4vect_mad_sse(int len, int vec, int vec_i, unsigned char *gftbls, unsigned char *src,
		      unsigned char **dest);

/**
 * @brief GF(2^8) vector multiply with 4 accumulate. AVX version of gf_4vect_mad_sse().
 * @requires AVX
 */
void gf_4vect_mad_avx(int len, int vec, int vec_i, unsigned char *gftbls, unsigned char *src,
		      unsigned char **dest);
/**
 * @brief GF(2^8) vector multiply with 4 accumulate. AVX2 version of gf_4vect_mad_sse().
 * @requires AVX2
 */
void gf_4vect_mad_avx2(int len, int vec, int vec_i, unsigned char *gftbls, unsigned char *src,
		       unsigned char **dest);

/**
 * @brief GF(2^8) vector multiply with 5 accumulate. SSE version.
 * @requires SSE4.1
 */
void gf_5vect_mad_sse(int len, int vec, int vec_i, unsigned char *gftbls, unsigned char *src,
		      unsigned char **dest);

/**
 * @brief GF(2^8) vector multiply with 5 accumulate. AVX version.
 * @requires AVX
 */
void gf_5vect_mad_avx(int len, int vec, int vec_i, unsigned char *gftbls, unsigned char *src,
		      unsigned char **dest);
/**
 * @brief GF(2^8) vector multiply with 5 accumulate. AVX2 version.
 * @requires AVX2
 */
void gf_5vect_mad_avx2(int len, int vec, int vec_i, unsigned char *gftbls, unsigned char *src,
		       unsigned char **dest);

/**
 * @brief GF(2^8) vector multiply with 6 accumulate. SSE version.
 * @requires SSE4.1
 */
void gf_6vect_mad_sse(int len, int vec, int vec_i, unsigned char *gftbls, unsigned char *src,
		      unsigned char **dest);
/**
 * @brief GF(2^8) vector multiply with 6 accumulate. AVX version.
 * @requires AVX
 */
void gf_6vect_mad_avx(int len, int vec, int vec_i, unsigned char *gftbls, unsigned char *src,
		      unsigned char **dest);

/**
 * @brief GF(2^8) vector multiply with 6 accumulate. AVX2 version.
 * @requires AVX2
 */
void gf_6vect_mad_avx2(int len, int vec, int vec_i, unsigned char *gftbls, unsigned char *src,
		       unsigned char **dest);


/**********************************************************************
 * The remaining are lib support functions used in GF(2^8) operations.
 */

/**
 * @brief Single element GF(2^8) multiply.
 *
 * @param a  Multiplicand a
 * @param b  Multiplicand b
 * @returns  Product of a and b in GF(2^8)
 */

unsigned char gf_mul(unsigned char a, unsigned char b);

/**
 * @brief Single element GF(2^8) inverse.
 *
 * @param a  Input element
 * @returns  Field element b such that a x b = {1}
 */

unsigned char gf_inv(unsigned char a);

/**
 * @brief Generate a matrix of coefficients to be used for encoding.
 *
 * Vandermonde matrix example of encoding coefficients where high portion of
 * matrix is identity matrix I and lower portion is constructed as 2^{i*(j-k+1)}
 * i:{0,k-1} j:{k,m-1}. Commonly used method for choosing coefficients in
 * erasure encoding but does not guarantee invertable for every sub matrix.  For
 * large k it is possible to find cases where the decode matrix chosen from
 * sources and parity not in erasure are not invertable. Users may want to
 * adjust for k > 5.
 *
 * @param a  [mxk] array to hold coefficients
 * @param m  number of rows in matrix corresponding to srcs + parity.
 * @param k  number of columns in matrix corresponding to srcs.
 * @returns  none
 */

void gf_gen_rs_matrix(unsigned char *a, int m, int k);

/**
 * @brief Generate a Cauchy matrix of coefficients to be used for encoding.
 *
 * Cauchy matrix example of encoding coefficients where high portion of matrix
 * is identity matrix I and lower portion is constructed as 1/(i + j) | i != j,
 * i:{0,k-1} j:{k,m-1}.  Any sub-matrix of a Cauchy matrix should be invertable.
 *
 * @param a  [mxk] array to hold coefficients
 * @param m  number of rows in matrix corresponding to srcs + parity.
 * @param k  number of columns in matrix corresponding to srcs.
 * @returns  none
 */

void gf_gen_cauchy1_matrix(unsigned char *a, int m, int k);

/**
 * @brief Invert a matrix in GF(2^8)
 *
 * @param in  input matrix
 * @param out output matrix such that [in] x [out] = [I] - identity matrix
 * @param n   size of matrix [nxn]
 * @returns 0 successful, other fail on singular input matrix
 */

int gf_invert_matrix(unsigned char *in, unsigned char *out, const int n);


/*************************************************************/

#ifdef __cplusplus
}
#endif

#endif //_ERASURE_CODE_H_
