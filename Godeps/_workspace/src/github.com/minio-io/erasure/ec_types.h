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


/**
 *  @file  types.h
 *  @brief Defines standard width types.
 *
 */

#ifndef __TYPES_H
#define __TYPES_H

#ifdef __cplusplus
extern "C" {
#endif

#ifndef __unix__
#ifdef __MINGW32__
# include <_mingw.h>
#endif
typedef unsigned __int64 UINT64;
typedef          __int64  INT64;
typedef unsigned __int32 UINT32;
typedef unsigned __int16 UINT16;
typedef unsigned char    UINT8;
#else
typedef unsigned long int  UINT64;
typedef          long int   INT64;
typedef unsigned int       UINT32;
typedef unsigned short int UINT16;
typedef unsigned char      UINT8;
#endif


#ifdef __unix__
# define DECLARE_ALIGNED(decl, alignval) decl __attribute__((aligned(alignval)))
# define __forceinline static inline
#else
# define DECLARE_ALIGNED(decl, alignval) __declspec(align(alignval)) decl
# define posix_memalign(p, algn, len) (NULL == (*((char**)(p)) = (void*) _aligned_malloc(len, algn)))
#endif

#ifdef DEBUG
# define DEBUG_PRINT(x) printf x
#else
# define DEBUG_PRINT(x) do {} while (0)
#endif

#ifdef __cplusplus
}
#endif

#endif  //__TYPES_H
