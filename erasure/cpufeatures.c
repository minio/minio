/*
 * Mini Object Storage, (C) 2014 Minio, Inc.
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

#include <stdio.h>
#include <stdlib.h>

static void cpuid(int cpuinfo[4] ,int infotype) {
        __asm__ __volatile__ (
                "cpuid":
                "=a" (cpuinfo[0]),
                "=b" (cpuinfo[1]),
                "=c" (cpuinfo[2]),
                "=d" (cpuinfo[3]) :
                "a" (infotype), "c" (0)
                );
}

/*
  SSE41 : return true
  no SSE41 : return false
*/
int has_sse41 (void) {
        int info[4];
        cpuid(info, 0x00000001);
        return ((info[2] & ((int)1 << 19)) != 0);
}

/*
  AVX : return true
  no AVX : return false
*/
int has_avx (void) {
        int info[4];
        cpuid(info, 0x00000001);
        return ((info[2] & ((int)1 << 28)) != 0);
}

/*
  AVX2 : return true
  no AVX2 : return false
*/
int has_avx2 (void) {
        int info[4];
        cpuid(info, 0x00000007);
        return ((info[1] & ((int)1 <<  5)) != 0);
}
