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


#ifndef _GF_VECT_MUL_H
#define _GF_VECT_MUL_H

/**
 *  @file gf_vect_mul.h
 *  @brief Interface to functions for vector (block) multiplication in GF(2^8).
 *
 *  This file defines the interface to routines used in fast RAID rebuild and 
 *  erasure codes.
 */

#ifdef __cplusplus
extern "C" {
#endif

 /**
 * @brief GF(2^8) vector multiply by constant.
 *
 * Does a GF(2^8) vector multiply b = Ca where a and b are arrays and C
 * is a single field element in GF(2^8). Can be used for RAID6 rebuild
 * and partial write functions. Function requires pre-calculation of a
 * 32-element constant array based on constant C. gftbl(C) = {C{00},
 * C{01}, C{02}, ... , C{0f} }, {C{00}, C{10}, C{20}, ... , C{f0} }. Len
 * and src must be aligned to 32B.
 * @requires SSE4.1
 *
 * @param len   Length of vector in bytes. Must be aligned to 32B.
 * @param gftbl Pointer to 32-byte array of pre-calculated constants based on C.
 * @param src   Pointer to src data array. Must be aligned to 32B.
 * @param dest  Pointer to destination data array. Must be aligned to 32B.
 * @returns 0 pass, other fail
 */

int gf_vect_mul_sse(int len, unsigned char *gftbl, void *src, void *dest);


 /**
 * @brief GF(2^8) vector multiply by constant.
 *
 * Does a GF(2^8) vector multiply b = Ca where a and b are arrays and C
 * is a single field element in GF(2^8). Can be used for RAID6 rebuild
 * and partial write functions. Function requires pre-calculation of a
 * 32-element constant array based on constant C. gftbl(C) = {C{00},
 * C{01}, C{02}, ... , C{0f} }, {C{00}, C{10}, C{20}, ... , C{f0} }. Len
 * and src must be aligned to 32B.
 * @requires AVX
 *
 * @param len   Length of vector in bytes. Must be aligned to 32B.
 * @param gftbl Pointer to 32-byte array of pre-calculated constants based on C.
 * @param src   Pointer to src data array. Must be aligned to 32B.
 * @param dest  Pointer to destination data array. Must be aligned to 32B.
 * @returns 0 pass, other fail
 */

int gf_vect_mul_avx(int len, unsigned char *gftbl, void *src, void *dest);


/**
 * @brief GF(2^8) vector multiply by constant, runs appropriate version.
 * 	
 * Does a GF(2^8) vector multiply b = Ca where a and b are arrays and C
 * is a single field element in GF(2^8). Can be used for RAID6 rebuild
 * and partial write functions. Function requires pre-calculation of a
 * 32-element constant array based on constant C. gftbl(C) = {C{00},
 * C{01}, C{02}, ... , C{0f} }, {C{00}, C{10}, C{20}, ... , C{f0} }.
 * Len and src must be aligned to 32B.
 *
 * This function determines what instruction sets are enabled 
 * and selects the appropriate version at runtime. 
 * 
 * @param len   Length of vector in bytes. Must be aligned to 32B.
 * @param gftbl Pointer to 32-byte array of pre-calculated constants based on C.
 * @param src   Pointer to src data array. Must be aligned to 32B.
 * @param dest  Pointer to destination data array. Must be aligned to 32B.
 * @returns 0 pass, other fail
 */

int gf_vect_mul(int len, unsigned char *gftbl, void *src, void *dest);


/**
 * @brief Initialize 32-byte constant array for GF(2^8) vector multiply
 *
 * Calculates array {C{00}, C{01}, C{02}, ... , C{0f} }, {C{00}, C{10},
 * C{20}, ... , C{f0} } as required by other fast vector multiply
 * functions.
 * @param c     Constant input.
 * @param gftbl Table output.
 */

void gf_vect_mul_init(unsigned char c, unsigned char* gftbl);


/**
 * @brief GF(2^8) vector multiply by constant, runs baseline version.
 *
 * Does a GF(2^8) vector multiply b = Ca where a and b are arrays and C
 * is a single field element in GF(2^8). Can be used for RAID6 rebuild
 * and partial write functions. Function requires pre-calculation of a
 * 32-element constant array based on constant C. gftbl(C) = {C{00},
 * C{01}, C{02}, ... , C{0f} }, {C{00}, C{10}, C{20}, ... , C{f0} }. Len
 * and src must be aligned to 32B.
 *
 * @param len   Length of vector in bytes. Must be aligned to 32B.
 * @param a 	Pointer to 32-byte array of pre-calculated constants based on C.
 * 		only use 2nd element is used.
 * @param src   Pointer to src data array. Must be aligned to 32B.
 * @param dest  Pointer to destination data array. Must be aligned to 32B.
 */

void gf_vect_mul_base(int len, unsigned char *a, unsigned char *src, 
			unsigned char *dest);

#ifdef __cplusplus
}
#endif

#endif //_GF_VECT_MUL_H
