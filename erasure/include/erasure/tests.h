/**********************************************************************
  Copyright(c) 2011-2014 Intel Corporation All rights reserved.

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


#ifndef __ERASURE_TESTS_H
#define __ERASURE_TESTS_H

#ifdef __cplusplus
extern "C" {
#endif

// Use sys/time.h functions for time

#include <sys/time.h>

struct perf{
	struct timeval tv;
};


inline int perf_start(struct perf *p)
{
	return gettimeofday(&(p->tv), 0);
}
inline int perf_stop(struct perf *p)
{
	return gettimeofday(&(p->tv), 0);
}

inline void perf_print(struct perf stop, struct perf start, long long dsize)
{
	long long secs = stop.tv.tv_sec - start.tv.tv_sec;
	long long usecs = secs * 1000000 + stop.tv.tv_usec - start.tv.tv_usec;

	printf("runtime = %10lld usecs", usecs);
	if (dsize != 0) {
#if 1 // not bug in printf for 32-bit
		printf(", bandwidth %lld MB in %.4f sec = %.2f MB/s\n", dsize/(1024*1024),
			((double) usecs)/1000000, ((double) dsize) / (double)usecs);
#else
		printf(", bandwidth %lld MB ", dsize/(1024*1024));
		printf("in %.4f sec ",(double)usecs/1000000);
		printf("= %.2f MB/s\n", (double)dsize/usecs);
#endif
	}
	else
		printf("\n");
}


#ifdef __cplusplus
}
#endif

#endif // __ERASURE_TESTS_H
