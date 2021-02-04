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
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import java.security.*;
import java.util.*;

import java.nio.file.*;
import java.math.BigInteger;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.SSECustomerKey;

// Main Testing class
public class FunctionalTests {

    private static final String PASS = "PASS";
    private static final String FAILED = "FAIL";
    private static final String IGNORED = "NA";

    private static String accessKey;
    private static String secretKey;
    private static String region;
    private static String endpoint;
    private static boolean enableHTTPS;

    private static final Random random = new Random(new SecureRandom().nextLong());
    private static String bucketName = getRandomName();
    private static boolean mintEnv = false;

    private static String file1Kb;
    private static String file1Mb;
    private static String file6Mb;

    private static SSECustomerKey sseKey1;
    private static SSECustomerKey sseKey2;
    private static SSECustomerKey sseKey3;

    private static AmazonS3 s3Client;
    private static S3TestUtils s3TestUtils;

    public static String getRandomName() {
        return "aws-java-sdk-test-" + new BigInteger(32, random).toString(32);
    }

    /**
     * Prints a success log entry in JSON format.
     */
    public static void mintSuccessLog(String function, String args, long startTime) {
        if (mintEnv) {
            System.out.println(
                    new MintLogger(function, args, System.currentTimeMillis() - startTime, PASS, null, null, null));
        }
    }

    /**
     * Prints a failure log entry in JSON format.
     */
    public static void mintFailedLog(String function, String args, long startTime, String message, String error) {
        if (mintEnv) {
            System.out.println(new MintLogger(function, args, System.currentTimeMillis() - startTime, FAILED, null,
                    message, error));
        }
    }

    /**
     * Prints a ignore log entry in JSON format.
     */
    public static void mintIgnoredLog(String function, String args, long startTime) {
        if (mintEnv) {
            System.out.println(
                    new MintLogger(function, args, System.currentTimeMillis() - startTime, IGNORED, null, null, null));
        }
    }

    public static void initTests() throws IOException {
        // Create encryption key.
        byte[] rawKey1 = "32byteslongsecretkeymustgenerate".getBytes();
        SecretKey secretKey1 = new SecretKeySpec(rawKey1, 0, rawKey1.length, "AES");
        sseKey1 = new SSECustomerKey(secretKey1);

        // Create new encryption key for target so it is saved using sse-c
        byte[] rawKey2 = "xxbytescopysecretkeymustprovided".getBytes();
        SecretKey secretKey2 = new SecretKeySpec(rawKey2, 0, rawKey2.length, "AES");
        sseKey2 = new SSECustomerKey(secretKey2);

        // Create new encryption key for target so it is saved using sse-c
        byte[] rawKey3 = "32byteslongsecretkeymustgenerat1".getBytes();
        SecretKey secretKey3 = new SecretKeySpec(rawKey3, 0, rawKey3.length, "AES");
        sseKey3 = new SSECustomerKey(secretKey3);

        // Create bucket
        s3Client.createBucket(new CreateBucketRequest(bucketName));
    }

    public static void teardown() throws IOException {

        // Remove all objects under the test bucket & the bucket itself
        // TODO: use multi delete API instead
        ObjectListing objectListing = s3Client.listObjects(bucketName);
        while (true) {
            for (Iterator<?> iterator = objectListing.getObjectSummaries().iterator(); iterator.hasNext();) {
                S3ObjectSummary summary = (S3ObjectSummary) iterator.next();
                s3Client.deleteObject(bucketName, summary.getKey());
            }
            // more objectListing to retrieve?
            if (objectListing.isTruncated()) {
                objectListing = s3Client.listNextBatchOfObjects(objectListing);
            } else {
                break;
            }
        }
        ;
        s3Client.deleteBucket(bucketName);
    }

    // Test regular object upload using encryption
    public static void uploadObjectEncryption_test1() throws Exception {
        if (!mintEnv) {
            System.out.println(
                    "Test: uploadObject(String bucketName, String objectName, String f, SSECustomerKey sseKey)");
        }

        if (!enableHTTPS) {
            return;
        }

        long startTime = System.currentTimeMillis();
        String file1KbMD5 = Utils.getFileMD5(file1Kb);
        String objectName = "testobject";
        try {
            s3TestUtils.uploadObject(bucketName, objectName, file1Kb, sseKey1);
            s3TestUtils.downloadObject(bucketName, objectName, sseKey1, file1KbMD5);
            mintSuccessLog("uploadObject(String bucketName, String objectName, String f, SSECustomerKey sseKey)",
                    "bucketName: " + bucketName + ", objectName: " + objectName + ", String: " + file1Kb
                            + ", SSECustomerKey: " + sseKey1,
                    startTime);
        } catch (Exception e) {
            mintFailedLog("uploadObject(String bucketName, String objectName, String f, SSECustomerKey sseKey)",
                    "bucketName: " + bucketName + ", objectName: " + objectName + ", String: " + file1Kb
                            + ", SSECustomerKey: " + sseKey1,
                    startTime, null, e.toString() + " >>> " + Arrays.toString(e.getStackTrace()));
            throw e;
        }
    }

    // Test downloading an object with a wrong encryption key
    public static void downloadObjectEncryption_test1() throws Exception {
        if (!mintEnv) {
            System.out.println("Test: downloadObject(String bucketName, String objectName, SSECustomerKey sseKey)");
        }

        if (!enableHTTPS) {
            return;
        }

        long startTime = System.currentTimeMillis();

        String file1KbMD5 = Utils.getFileMD5(file1Kb);
        String objectName = "testobject";

        try {
            s3TestUtils.uploadObject(bucketName, "testobject", file1Kb, sseKey1);
            s3TestUtils.downloadObject(bucketName, objectName, sseKey2);
            Exception ex = new Exception("downloadObject did not throw an S3 Access denied exception");
            mintFailedLog("downloadObject(String bucketName, String objectName, SSECustomerKey sseKey)",
                    "bucketName: " + bucketName + ", objectName: " + objectName + ", SSECustomerKey: " + sseKey2,
                    startTime, null, ex.toString() + " >>> " + Arrays.toString(ex.getStackTrace()));
            throw ex;
        } catch (Exception e) {
            if (!e.getMessage().contains("Access Denied")) {
                Exception ex = new Exception(
                        "downloadObject did not throw S3 Access denied Exception but it did throw: " + e.getMessage());
                mintFailedLog("downloadObject(String bucketName, String objectName, SSECustomerKey sseKey)",
                        "bucketName: " + bucketName + ", objectName: " + objectName + ", SSECustomerKey: " + sseKey2,
                        startTime, null, ex.toString() + " >>> " + Arrays.toString(ex.getStackTrace()));
                throw ex;
            }
            mintSuccessLog("downloadObject(String bucketName, String objectName, SSECustomerKey sseKey)",
                    "bucketName: " + bucketName + ", objectName: " + objectName + ", SSECustomerKey: " + sseKey2,
                    startTime);
        }
    }

    // Test copying object with a new different encryption key
    public static void copyObjectEncryption_test1() throws Exception {
        if (!mintEnv) {
            System.out.println("Test: copyObject(String bucketName, String objectName, SSECustomerKey sseKey, "
                    + "String destBucketName, String dstObjectName, SSECustomerKey sseKey2, boolean replaceDirective)");
        }

        if (!enableHTTPS) {
            return;
        }

        long startTime = System.currentTimeMillis();
        String file1KbMD5 = Utils.getFileMD5(file1Kb);
        String objectName = "testobject";
        String dstObjectName = "dir/newobject";

        try {
            s3TestUtils.uploadObject(bucketName, objectName, file1Kb, sseKey1);
            s3TestUtils.copyObject(bucketName, objectName, sseKey1, bucketName, dstObjectName, sseKey2, false);
            s3TestUtils.downloadObject(bucketName, dstObjectName, sseKey2, file1KbMD5);
        } catch (Exception e) {
            mintFailedLog("copyObject(String bucketName, String objectName, SSECustomerKey sseKey, "
                    + "String destBucketName, String dstObjectName, SSECustomerKey sseKey2, boolean replaceDirective)",
                    "bucketName: " + bucketName + ", objectName: " + objectName + ", SSECustomerKey: " + sseKey1
                            + "DstbucketName: " + bucketName + ", DstObjectName: " + dstObjectName
                            + ", SSECustomerKey: " + sseKey2 + ", replaceDirective: " + false,
                    startTime, null, e.toString() + " >>> " + Arrays.toString(e.getStackTrace()));
            throw e;
        }
        mintSuccessLog("copyObject(String bucketName, String objectName, SSECustomerKey sseKey, "
                + "String destBucketName, String dstObjectName, SSECustomerKey sseKey2, boolean replaceDirective)",
                "bucketName: " + bucketName + ", objectName: " + objectName + ", SSECustomerKey: " + sseKey1
                        + "DstbucketName: " + bucketName + ", DstObjectName: " + dstObjectName + ", SSECustomerKey: "
                        + sseKey2 + ", replaceDirective: " + false,
                startTime);
    }

    // Test copying object with wrong source encryption key
    public static void copyObjectEncryption_test2() throws Exception {
        if (!mintEnv) {
            System.out.println("Test: copyObject(String bucketName, String objectName, SSECustomerKey sseKey, "
                    + "String destBucketName, String dstObjectName, SSECustomerKey sseKey2, boolean replaceDirective)");
        }

        if (!enableHTTPS) {
            return;
        }

        String objectName = "testobject";
        String dstObjectName = "dir/newobject";

        long startTime = System.currentTimeMillis();

        try {
            s3TestUtils.copyObject(bucketName, objectName, sseKey3, bucketName, dstObjectName, sseKey2, false);
            Exception ex = new Exception("copyObject did not throw an S3 Access denied exception");
            mintFailedLog("copyObject(String bucketName, String objectName, SSECustomerKey sseKey, "
                    + "String destBucketName, String dstObjectName, SSECustomerKey sseKey2, boolean replaceDirective)",
                    "bucketName: " + bucketName + ", objectName: " + objectName + ", SSECustomerKey: " + sseKey3
                            + "DstbucketName: " + bucketName + ", DstObjectName: " + dstObjectName
                            + ", SSECustomerKey: " + sseKey2 + ", replaceDirective: " + false,
                    startTime, null, ex.toString() + " >>> " + Arrays.toString(ex.getStackTrace()));
            throw ex;
        } catch (Exception e) {
            if (!e.getMessage().contains("Access Denied")) {
                Exception ex = new Exception(
                        "copyObject did not throw S3 Access denied Exception but it did throw: " + e.getMessage());
                mintFailedLog("copyObject(String bucketName, String objectName, SSECustomerKey sseKey, "
                        + "String destBucketName, String dstObjectName, SSECustomerKey sseKey2, boolean replaceDirective)",
                        "bucketName: " + bucketName + ", objectName: " + objectName + ", SSECustomerKey: " + sseKey3
                                + "DstbucketName: " + bucketName + ", DstObjectName: " + dstObjectName
                                + ", SSECustomerKey: " + sseKey2 + ", replaceDirective: " + false,
                        startTime, null, ex.toString() + " >>> " + Arrays.toString(ex.getStackTrace()));
                throw ex;
            }
            mintSuccessLog("copyObject(String bucketName, String objectName, SSECustomerKey sseKey, "
                    + "String destBucketName, String dstObjectName, SSECustomerKey sseKey2, boolean replaceDirective)",
                    "bucketName: " + bucketName + ", objectName: " + objectName + ", SSECustomerKey: " + sseKey3
                            + "DstbucketName: " + bucketName + ", DstObjectName: " + dstObjectName
                            + ", SSECustomerKey: " + sseKey2 + ", replaceDirective: " + false,
                    startTime);
        }
    }

    // Test copying multipart object
    public static void copyObjectEncryption_test3() throws Exception {
        if (!mintEnv) {
            System.out.println("Test: copyObject(String bucketName, String objectName, SSECustomerKey sseKey, "
                    + "String destBucketName, String dstObjectName, SSECustomerKey sseKey2, boolean replaceDirective)");
        }

        if (!enableHTTPS) {
            return;
        }

        long startTime = System.currentTimeMillis();
        String file6MbMD5 = Utils.getFileMD5(file6Mb);
        String objectName = "testobject";
        String dstObjectName = "dir/newobject";

        try {
            s3TestUtils.uploadMultipartObject(bucketName, objectName, file6Mb, sseKey1);
            s3TestUtils.copyObject(bucketName, objectName, sseKey1, bucketName, dstObjectName, sseKey2, false);
            s3TestUtils.downloadObject(bucketName, dstObjectName, sseKey2, file6MbMD5);
        } catch (Exception e) {
            mintFailedLog("copyObject(String bucketName, String objectName, SSECustomerKey sseKey, "
                    + "String destBucketName, String dstObjectName, SSECustomerKey sseKey2, boolean replaceDirective)",
                    "bucketName: " + bucketName + ", objectName: " + objectName + ", SSECustomerKey: " + sseKey1
                            + "DstbucketName: " + bucketName + ", DstObjectName: " + dstObjectName
                            + ", SSECustomerKey: " + sseKey2 + ", replaceDirective: " + false,
                    startTime, null, e.toString() + " >>> " + Arrays.toString(e.getStackTrace()));
            throw e;
        }
        mintSuccessLog("copyObject(String bucketName, String objectName, SSECustomerKey sseKey, "
                + "String destBucketName, String dstObjectName, SSECustomerKey sseKey2, boolean replaceDirective)",
                "bucketName: " + bucketName + ", objectName: " + objectName + ", SSECustomerKey: " + sseKey1
                        + "DstbucketName: " + bucketName + ", DstObjectName: " + dstObjectName + ", SSECustomerKey: "
                        + sseKey2 + ", replaceDirective: " + false,
                startTime);
    }

    // Test downloading encrypted object with Get Range, 0 -> 1024
    public static void downloadGetRangeEncryption_test1() throws Exception {
        if (!mintEnv) {
            System.out.println("Test: downloadObjectGetRange(String bucketName, String objectName, "
                    + "SSECustomerKey sseKey, String expectedMD5, int start, int length)");
        }

        if (!enableHTTPS) {
            return;
        }

        long startTime = System.currentTimeMillis();

        String objectName = "testobject";
        String range1MD5 = Utils.getFileMD5(file1Kb);
        int start = 0;
        int length = 1024;
        try {
            s3TestUtils.uploadObject(bucketName, objectName, file1Kb, sseKey1);
            s3TestUtils.downloadObject(bucketName, objectName, sseKey1, range1MD5, start, length);
        } catch (Exception e) {
            mintFailedLog(
                    "downloadObjectGetRange(String bucketName, String objectName, "
                            + "SSECustomerKey sseKey, String expectedMD5, int start, int length)",
                    "bucketName: " + bucketName + ", objectName: " + objectName + ", SSECustomerKey: " + sseKey1
                            + ", expectedMD5: " + range1MD5 + ", start: " + start + ", length: " + length,
                    startTime, null, e.toString() + " >>> " + Arrays.toString(e.getStackTrace()));
            throw e;
        }
        mintSuccessLog(
                "downloadObjectGetRange(String bucketName, String objectName, "
                        + "SSECustomerKey sseKey, String expectedMD5, int start, int length)",
                "bucketName: " + bucketName + ", objectName: " + objectName + ", SSECustomerKey: " + sseKey1
                        + ", expectedMD5: " + range1MD5 + ", start: " + start + ", length: " + length,
                startTime);
    }

    // Test downloading encrypted object with Get Range, 0 -> 1
    public static void downloadGetRangeEncryption_test2() throws Exception {
        if (!mintEnv) {
            System.out.println("Test: downloadObjectGetRange(String bucketName, String objectName, "
                    + "SSECustomerKey sseKey, String expectedMD5, int start, int length)");
        }

        if (!enableHTTPS) {
            return;
        }

        long startTime = System.currentTimeMillis();

        String objectName = "testobject";
        int start = 0;
        int length = 1;
        String range1MD5 = Utils.getFileMD5(file1Kb, start, length);
        try {
            s3TestUtils.uploadObject(bucketName, objectName, file1Kb, sseKey1);
            s3TestUtils.downloadObject(bucketName, objectName, sseKey1, range1MD5, start, length);
        } catch (Exception e) {
            mintFailedLog(
                    "downloadObjectGetRange(String bucketName, String objectName, "
                            + "SSECustomerKey sseKey, String expectedMD5, int start, int length)",
                    "bucketName: " + bucketName + ", objectName: " + objectName + ", SSECustomerKey: " + sseKey1
                            + ", expectedMD5: " + range1MD5 + ", start: " + start + ", length: " + length,
                    startTime, null, e.toString() + " >>> " + Arrays.toString(e.getStackTrace()));
            throw e;
        }
        mintSuccessLog(
                "downloadObjectGetRange(String bucketName, String objectName, "
                        + "SSECustomerKey sseKey, String expectedMD5, int start, int length)",
                "bucketName: " + bucketName + ", objectName: " + objectName + ", SSECustomerKey: " + sseKey1
                        + ", expectedMD5: " + range1MD5 + ", start: " + start + ", length: " + length,
                startTime);
    }

    // Test downloading encrypted object with Get Range, 0 -> 1024-1
    public static void downloadGetRangeEncryption_test3() throws Exception {
        if (!mintEnv) {
            System.out.println("Test: downloadObjectGetRange(String bucketName, String objectName, "
                    + "SSECustomerKey sseKey, String expectedMD5, int start, int length)");
        }

        if (!enableHTTPS) {
            return;
        }

        long startTime = System.currentTimeMillis();

        String objectName = "testobject";
        int start = 0;
        int length = 1023;
        String range1MD5 = Utils.getFileMD5(file1Kb, start, length);
        try {
            s3TestUtils.uploadObject(bucketName, objectName, file1Kb, sseKey1);
            s3TestUtils.downloadObject(bucketName, objectName, sseKey1, range1MD5, start, length);
        } catch (Exception e) {
            mintFailedLog(
                    "downloadObjectGetRange(String bucketName, String objectName, "
                            + "SSECustomerKey sseKey, String expectedMD5, int start, int length)",
                    "bucketName: " + bucketName + ", objectName: " + objectName + ", SSECustomerKey: " + sseKey1
                            + ", expectedMD5: " + range1MD5 + ", start: " + start + ", length: " + length,
                    startTime, null, e.toString() + " >>> " + Arrays.toString(e.getStackTrace()));
            throw e;
        }
        mintSuccessLog(
                "downloadObjectGetRange(String bucketName, String objectName, "
                        + "SSECustomerKey sseKey, String expectedMD5, int start, int length)",
                "bucketName: " + bucketName + ", objectName: " + objectName + ", SSECustomerKey: " + sseKey1
                        + ", expectedMD5: " + range1MD5 + ", start: " + start + ", length: " + length,
                startTime);
    }

    // Test downloading encrypted object with Get Range, 1 -> 1024-1
    public static void downloadGetRangeEncryption_test4() throws Exception {
        if (!mintEnv) {
            System.out.println("Test: downloadObjectGetRange(String bucketName, String objectName, "
                    + "SSECustomerKey sseKey, String expectedMD5, int start, int length)");
        }

        if (!enableHTTPS) {
            return;
        }

        long startTime = System.currentTimeMillis();

        String objectName = "testobject";
        int start = 1;
        int length = 1023;
        String range1MD5 = Utils.getFileMD5(file1Kb, start, length);
        try {
            s3TestUtils.uploadObject(bucketName, objectName, file1Kb, sseKey1);
            s3TestUtils.downloadObject(bucketName, objectName, sseKey1, range1MD5, start, length);
        } catch (Exception e) {
            mintFailedLog(
                    "downloadObjectGetRange(String bucketName, String objectName, "
                            + "SSECustomerKey sseKey, String expectedMD5, int start, int length)",
                    "bucketName: " + bucketName + ", objectName: " + objectName + ", SSECustomerKey: " + sseKey1
                            + ", expectedMD5: " + range1MD5 + ", start: " + start + ", length: " + length,
                    startTime, null, e.toString() + " >>> " + Arrays.toString(e.getStackTrace()));
            throw e;
        }
        mintSuccessLog(
                "downloadObjectGetRange(String bucketName, String objectName, "
                        + "SSECustomerKey sseKey, String expectedMD5, int start, int length)",
                "bucketName: " + bucketName + ", objectName: " + objectName + ", SSECustomerKey: " + sseKey1
                        + ", expectedMD5: " + range1MD5 + ", start: " + start + ", length: " + length,
                startTime);
    }

    // Test downloading encrypted object with Get Range, 64*1024 -> 64*1024
    public static void downloadGetRangeEncryption_test5() throws Exception {
        if (!mintEnv) {
            System.out.println("Test: downloadObjectGetRange(String bucketName, String objectName, "
                    + "SSECustomerKey sseKey, String expectedMD5, int start, int length)");
        }

        if (!enableHTTPS) {
            return;
        }

        long startTime = System.currentTimeMillis();

        String objectName = "testobject";
        int start = 64 * 1024;
        int length = 64 * 1024;
        String range1MD5 = Utils.getFileMD5(file1Mb, start, length);
        try {
            s3TestUtils.uploadObject(bucketName, objectName, file1Mb, sseKey1);
            s3TestUtils.downloadObject(bucketName, objectName, sseKey1, range1MD5, start, length);
        } catch (Exception e) {
            mintFailedLog(
                    "downloadObjectGetRange(String bucketName, String objectName, "
                            + "SSECustomerKey sseKey, String expectedMD5, int start, int length)",
                    "bucketName: " + bucketName + ", objectName: " + objectName + ", SSECustomerKey: " + sseKey1
                            + ", expectedMD5: " + range1MD5 + ", start: " + start + ", length: " + length,
                    startTime, null, e.toString() + " >>> " + Arrays.toString(e.getStackTrace()));
            throw e;
        }
        mintSuccessLog(
                "downloadObjectGetRange(String bucketName, String objectName, "
                        + "SSECustomerKey sseKey, String expectedMD5, int start, int length)",
                "bucketName: " + bucketName + ", objectName: " + objectName + ", SSECustomerKey: " + sseKey1
                        + ", expectedMD5: " + range1MD5 + ", start: " + start + ", length: " + length,
                startTime);
    }

    // Test downloading encrypted object with Get Range, 64*1024 ->
    // 1024*1024-64*1024
    public static void downloadGetRangeEncryption_test6() throws Exception {
        if (!mintEnv) {
            System.out.println("Test: downloadObjectGetRange(String bucketName, String objectName, "
                    + "SSECustomerKey sseKey, String expectedMD5, int start, int length)");
        }

        if (!enableHTTPS) {
            return;
        }

        long startTime = System.currentTimeMillis();

        String objectName = "testobject";
        int start = 64 * 1024;
        int length = 1024 * 1024 - 64 * 1024;
        String range1MD5 = Utils.getFileMD5(file1Mb, start, length);
        try {
            s3TestUtils.uploadObject(bucketName, objectName, file1Mb, sseKey1);
            s3TestUtils.downloadObject(bucketName, objectName, sseKey1, range1MD5, start, length);
        } catch (Exception e) {
            mintFailedLog(
                    "downloadObjectGetRange(String bucketName, String objectName, "
                            + "SSECustomerKey sseKey, String expectedMD5, int start, int length)",
                    "bucketName: " + bucketName + ", objectName: " + objectName + ", SSECustomerKey: " + sseKey1
                            + ", expectedMD5: " + range1MD5 + ", start: " + start + ", length: " + length,
                    startTime, null, e.toString() + " >>> " + Arrays.toString(e.getStackTrace()));
            throw e;
        }
        mintSuccessLog(
                "downloadObjectGetRange(String bucketName, String objectName, "
                        + "SSECustomerKey sseKey, String expectedMD5, int start, int length)",
                "bucketName: " + bucketName + ", objectName: " + objectName + ", SSECustomerKey: " + sseKey1
                        + ", expectedMD5: " + range1MD5 + ", start: " + start + ", length: " + length,
                startTime);
    }

    // Run tests
    public static void runTests() throws Exception {

        uploadObjectEncryption_test1();

        downloadObjectEncryption_test1();

        copyObjectEncryption_test1();
        copyObjectEncryption_test2();
        copyObjectEncryption_test3();

        downloadGetRangeEncryption_test1();
        downloadGetRangeEncryption_test2();
        downloadGetRangeEncryption_test3();
        downloadGetRangeEncryption_test4();
        downloadGetRangeEncryption_test5();
        downloadGetRangeEncryption_test6();
    }

    public static void main(String[] args) throws Exception, IOException, NoSuchAlgorithmException {

        endpoint = System.getenv("SERVER_ENDPOINT");
        accessKey = System.getenv("ACCESS_KEY");
        secretKey = System.getenv("SECRET_KEY");
        enableHTTPS = System.getenv("ENABLE_HTTPS").equals("1");

        region = "us-east-1";

        if (enableHTTPS) {
            endpoint = "https://" + endpoint;
        } else {
            endpoint = "http://" + endpoint;
        }

        String dataDir = System.getenv("MINT_DATA_DIR");
        if (dataDir != null && !dataDir.equals("")) {
            mintEnv = true;
            file1Kb = Paths.get(dataDir, "datafile-1-kB").toString();
            file1Mb = Paths.get(dataDir, "datafile-1-MB").toString();
            file6Mb = Paths.get(dataDir, "datafile-6-MB").toString();
        }

        String mintMode = null;
        if (mintEnv) {
            mintMode = System.getenv("MINT_MODE");
        }

        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        AmazonS3ClientBuilder.EndpointConfiguration endpointConfiguration = new AmazonS3ClientBuilder.EndpointConfiguration(
                endpoint, region);

        AmazonS3ClientBuilder clientBuilder = AmazonS3ClientBuilder.standard();
        clientBuilder.setCredentials(new AWSStaticCredentialsProvider(credentials));
        clientBuilder.setEndpointConfiguration(endpointConfiguration);
        clientBuilder.setPathStyleAccessEnabled(true);

        s3Client = clientBuilder.build();
        s3TestUtils = new S3TestUtils(s3Client);

        try {
            initTests();
            FunctionalTests.runTests();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        } finally {
            teardown();
        }
    }
}
