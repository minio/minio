/*
 * Copyright (c) 2015-2021 MinIO, Inc.
 *
 * This file is part of MinIO Object Storage stack
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
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

