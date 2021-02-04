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
import java.util.*;
import java.nio.channels.Channels;

import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.SSECustomerKey;

import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;

import com.amazonaws.services.s3.model.MetadataDirective;

import com.amazonaws.services.s3.AmazonS3;

class S3TestUtils {

    private AmazonS3 s3Client;

    S3TestUtils(AmazonS3 s3Client) {
        this.s3Client = s3Client;
    }

    void uploadMultipartObject(String bucketName, String keyName,
            String filePath, SSECustomerKey sseKey) throws IOException {

        File file = new File(filePath);

        List<PartETag> partETags = new ArrayList<PartETag>();

        // Step 1: Initialize.
        InitiateMultipartUploadRequest initRequest = new
             InitiateMultipartUploadRequest(bucketName, keyName);

        if (sseKey != null) {
            initRequest.setSSECustomerKey(sseKey);
        }

        InitiateMultipartUploadResult initResponse =
        	                   s3Client.initiateMultipartUpload(initRequest);

        long contentLength = file.length();
        long partSize = 5242880; // Set part size to 5 MB.

        // Step 2: Upload parts.
        long filePosition = 0;
        for (int i = 1; filePosition < contentLength; i++) {
            // Last part can be less than 5 MB. Adjust part size.
            partSize = Math.min(partSize, (contentLength - filePosition));

            // Create request to upload a part.
            UploadPartRequest uploadRequest = new UploadPartRequest()
                .withBucketName(bucketName).withKey(keyName)
                .withUploadId(initResponse.getUploadId()).withPartNumber(i)
                .withFileOffset(filePosition)
                .withFile(file)
                .withPartSize(partSize);

            if (sseKey != null) {
                uploadRequest.withSSECustomerKey(sseKey);
            }

            // Upload part and add response to our list.
            partETags.add(s3Client.uploadPart(uploadRequest).getPartETag());

            filePosition += partSize;
        }

        // Step 3: Complete.
        CompleteMultipartUploadRequest compRequest = new
            CompleteMultipartUploadRequest(
                    bucketName,
                    keyName,
                    initResponse.getUploadId(),
                    partETags);

        s3Client.completeMultipartUpload(compRequest);
    }

    void uploadObject(String bucketName, String keyName,
            String filePath, SSECustomerKey sseKey) throws IOException {

        File f = new File(filePath);
        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, keyName, f);
        if (sseKey != null) {
            putObjectRequest.withSSECustomerKey(sseKey);
        }
        s3Client.putObject(putObjectRequest);
    }

    void downloadObject(String bucketName, String keyName, SSECustomerKey sseKey)
        throws Exception, IOException {
        downloadObject(bucketName, keyName, sseKey, "", -1, -1);
    }

    void downloadObject(String bucketName, String keyName, SSECustomerKey sseKey,
            String expectedMD5)
        throws Exception, IOException {
        downloadObject(bucketName, keyName, sseKey, expectedMD5, -1, -1);
    }

    void downloadObject(String bucketName, String keyName, SSECustomerKey sseKey,
            String expectedMD5, int start, int length) throws Exception, IOException {
        GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, keyName)
            .withSSECustomerKey(sseKey);

        if (start >= 0 && length >= 0) {
            getObjectRequest.setRange(start, start+length-1);
        }

        S3Object s3Object = s3Client.getObject(getObjectRequest);

        int size = 0;
        int c;

        S3ObjectInputStream input = s3Object.getObjectContent();

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        String data = "";
        while ((c = input.read()) != -1) {
            output.write((byte) c);
            size++;
        }

        if (length >= 0 && size != length) {
            throw new Exception("downloaded object has unexpected size, expected: " + length + ", received: " + size);
        }

        String calculatedMD5 = Utils.getBufferMD5(output.toByteArray());

        if (!expectedMD5.equals("") && !calculatedMD5.equals(expectedMD5)) {
            throw new Exception("downloaded object has unexpected md5sum, expected: " + expectedMD5 + ", found: " + calculatedMD5);

        }
    }

    void copyObject(String bucketName, String keyName, SSECustomerKey sseKey,
            String targetBucketName, String targetKeyName, SSECustomerKey newSseKey,
            boolean replace) {
        CopyObjectRequest copyRequest = new CopyObjectRequest(bucketName, keyName, targetBucketName, targetKeyName);
        if (sseKey != null) {
            copyRequest.withSourceSSECustomerKey(sseKey);
        }
        if (newSseKey != null) {
            copyRequest.withDestinationSSECustomerKey(newSseKey);
        }
        if (replace) {
            copyRequest.withMetadataDirective(MetadataDirective.COPY);
        }
        s3Client.copyObject(copyRequest);
    }

    long retrieveObjectMetadata(String bucketName, String keyName, SSECustomerKey sseKey) {
        GetObjectMetadataRequest getMetadataRequest = new GetObjectMetadataRequest(bucketName, keyName)
            .withSSECustomerKey(sseKey);
        ObjectMetadata objectMetadata =  s3Client.getObjectMetadata(getMetadataRequest);
        return objectMetadata.getContentLength();
    }

}
