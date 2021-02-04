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
import java.nio.channels.*;
import java.security.*;

class Utils {

    public static byte[] createChecksum(InputStream is, int skip, int length) throws Exception {
        int numRead;
        byte[] buffer = new byte[1024];

        MessageDigest complete = MessageDigest.getInstance("MD5");

        if (skip > -1 && length > -1) {
            is = new LimitedInputStream(is, skip, length);
        }

        do {
            numRead = is.read(buffer);
            if (numRead > 0) {
                complete.update(buffer, 0, numRead);
            }
        } while (numRead != -1);

        return complete.digest();
    }

    public static String getInputStreamMD5(InputStream is) throws Exception {
        return getInputStreamMD5(is, -1, -1);
    }

    public static String getInputStreamMD5(InputStream is, int start, int length) throws Exception {
        byte[] b = createChecksum(is, start, length);
        String result = "";

        for (int i=0; i < b.length; i++) {
            result += Integer.toString( ( b[i] & 0xff ) + 0x100, 16).substring( 1 );
        }
        return result;
    }

    public static String getFileMD5(String filePath) throws Exception {
        return getFileMD5(filePath, -1, -1);
    }

    public static String getFileMD5(String filePath, int start, int length) throws Exception {
        File f = new File(filePath);
        InputStream is = new FileInputStream(f);
        return getInputStreamMD5(is, start, length);
    }

    public static String getBufferMD5(byte[] data) throws Exception {
        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        return getInputStreamMD5(bis);
    }
}

