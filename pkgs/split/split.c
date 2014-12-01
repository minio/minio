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

#include <sys/param.h>
#include <sys/types.h>

#include <ctype.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <assert.h>
#include "split.h"

static
size_t _get_filesize(int fd)
{
        struct stat st;
        assert(fstat(fd, &st) != -1);
        return (ssize_t) st.st_size;
}

static
ssize_t read_write_chunk(int in, int out, ssize_t bytes)
{
        ssize_t n, m;
        char *buf = NULL;

        buf = calloc(bytes, 1);
        assert(buf != NULL);

        n = read(in, buf, bytes);
        if (n < 0)
                return -1;

        m = write(out, buf, n);
        if (m < 0)
                return -1;

        if (buf)
                free(buf);
        return m;
}

static int
_allocate_newchunk(char *chunkname)
{

        int chunk = -1;

        if (!chunkname)
                return -1;

        if ((strlen(chunkname)) >= MAXPATHLEN) {
                fprintf (stderr, "chunkname + suffix too long");
                return -1;
        }

        chunk = open (chunkname, O_CREAT|O_WRONLY, S_IRUSR|S_IWUSR);
        return chunk;
}

/*
 * Generate chunks by chunking at input bytes.
 */
int
minio_split(char *filename, ssize_t bytecnt)
{
	ssize_t fsize;
        int output = -1;
        int input = -1;
        int remainder = 0;
        char newchunk[MAXPATHLEN] = {0,};
        int chunk_size = 0;
        int i = 0;

        if (bytecnt < 0)
                return -1;

        if ((input = open(filename, O_RDONLY)) < 0)
                return -1;

        fsize = _get_filesize(input);
        remainder = fsize % bytecnt;

        if (remainder == 0)
                chunk_size = fsize / bytecnt;
        else
                chunk_size = (fsize + (bytecnt - remainder)) / bytecnt;

        if (chunk_size == 0)
                return -1;

        for (i = 0; i < chunk_size; i++) {
                snprintf (newchunk, sizeof(newchunk)-1, "%s.%d", filename, i);

                if ((output = _allocate_newchunk(newchunk)) < 0)
                        return -1;

                if (read_write_chunk(input, output, bytecnt) < 0)
                        return -1;
        }
        return 0;
}
