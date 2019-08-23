/*
*  Mint, (C) 2018 Minio, Inc.
*
*  Licensed under the Apache License, Version 2.0 (the "License");
*  you may not use this file except in compliance with the License.
*  You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
*  Unless required by applicable law or agreed to in writing, software
*
*  distributed under the License is distributed on an "AS IS" BASIS,
*  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*  See the License for the specific language governing permissions and
*  limitations under the License.
*/

package io.minio.awssdk.tests;

import java.io.*;

// LimitedInputStream wraps a regular InputStream, calling
// read() will skip some bytes as configured and will also
// return only data with configured length

class LimitedInputStream extends InputStream {

    private int skip;
    private int length;
    private InputStream is;

    LimitedInputStream(InputStream is, int skip, int length) {
        this.is = is;
        this.skip = skip;
        this.length = length;
    }

    @Override
    public int read() throws IOException {
        int r;
        while (skip > 0) {
            r = is.read();
            if (r < 0) {
                throw new IOException("stream ended before being able to skip all bytes");
            }
            skip--;
        }
        if (length == 0) {
            return -1;
        }
        r = is.read();
        if (r < 0) {
            throw new IOException("stream ended before being able to read all bytes");
        }
        length--;
        return r;
    }
}


