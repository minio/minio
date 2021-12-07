// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"context"
	"crypto/md5"
	"flag"
	"fmt"
	"io"
	"log"
	"net/url"
	"strconv"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var (
	endpoint, accessKey, secretKey string
	bucket, prefix                 string
)

// getMD5Sum returns MD5 sum of given data.
func getMD5Sum(data []byte) []byte {
	hash := md5.New()
	hash.Write(data)
	return hash.Sum(nil)
}

func main() {
	flag.StringVar(&endpoint, "endpoint", "https://play.min.io", "S3 endpoint URL")
	flag.StringVar(&accessKey, "access-key", "Q3AM3UQ867SPQQA43P2F", "S3 Access Key")
	flag.StringVar(&secretKey, "secret-key", "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG", "S3 Secret Key")
	flag.StringVar(&bucket, "bucket", "", "Select a specific bucket")
	flag.StringVar(&prefix, "prefix", "", "Select a prefix")
	flag.Parse()

	if endpoint == "" {
		log.Fatalln("Endpoint is not provided")
	}

	if accessKey == "" {
		log.Fatalln("Access key is not provided")
	}

	if secretKey == "" {
		log.Fatalln("Secret key is not provided")
	}

	if bucket == "" && prefix != "" {
		log.Fatalln("--prefix is specified without --bucket.")
	}

	u, err := url.Parse(endpoint)
	if err != nil {
		log.Fatalln(err)
	}

	s3Client, err := minio.New(u.Host, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: strings.EqualFold(u.Scheme, "https"),
	})
	if err != nil {
		log.Fatalln()
	}

	// s3Client.TraceOn(os.Stderr)

	var buckets []string
	if bucket != "" {
		buckets = append(buckets, bucket)
	} else {
		bucketsInfo, err := s3Client.ListBuckets(context.Background())
		if err != nil {
			log.Fatalln(err)
		}
		for _, b := range bucketsInfo {
			buckets = append(buckets, b.Name)
		}
	}

	for _, bucket := range buckets {
		opts := minio.ListObjectsOptions{
			Recursive:    true,
			Prefix:       prefix,
			WithVersions: true,
		}

		// List all objects from a bucket-name with a matching prefix.
		for object := range s3Client.ListObjects(context.Background(), bucket, opts) {
			if object.Err != nil {
				log.Fatalln("LIST error:", object.Err)
				continue
			}
			if object.IsDeleteMarker {
				continue
			}
			parts := 1
			s := strings.Split(object.ETag, "-")
			switch len(s) {
			case 1:
				// nothing to do
			case 2:
				if p, err := strconv.Atoi(s[1]); err == nil {
					parts = p
				} else {
					log.Fatalln("ETAG: wrong format:", err)
					continue
				}
			default:
				log.Fatalln("Unexpected ETAG format", object.ETag)
			}

			var partsMD5Sum [][]byte

			for p := 1; p <= parts; p++ {
				obj, err := s3Client.GetObject(context.Background(), bucket, object.Key,
					minio.GetObjectOptions{VersionID: object.VersionID, PartNumber: p})
				if err != nil {
					log.Fatalln("GET", bucket, object.Key, object.VersionID, "=>", err)
					continue
				}
				h := md5.New()
				if _, err := io.Copy(h, obj); err != nil {
					log.Fatalln("MD5 calculation error:", bucket, object.Key, object.VersionID, "=>", err)
					continue
				}
				partsMD5Sum = append(partsMD5Sum, h.Sum(nil))
			}

			corrupted := false

			switch parts {
			case 1:
				md5sum := fmt.Sprintf("%x", partsMD5Sum[0])
				if md5sum != object.ETag {
					corrupted = true
				}
			default:
				var totalMD5SumBytes []byte
				for _, sum := range partsMD5Sum {
					totalMD5SumBytes = append(totalMD5SumBytes, sum...)
				}
				s3MD5 := fmt.Sprintf("%x-%d", getMD5Sum(totalMD5SumBytes), parts)
				if s3MD5 != object.ETag {
					corrupted = true
				}
			}

			if corrupted {
				log.Fatalln("CORRUPTED object:", bucket, object.Key, object.VersionID)
			}

			log.Println("INTACT", bucket, object.Key, object.VersionID)
		}
	}
}
