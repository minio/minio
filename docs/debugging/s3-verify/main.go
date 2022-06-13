// Copyright (c) 2015-2022 MinIO, Inc.
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
	"bytes"
	"context"
	"crypto/sha256"
	"flag"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"strings"
	"sync"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var (
	sourceEndpoint, sourceAccessKey, sourceSecretKey string
	sourceBucket, sourcePrefix                       string
	targetEndpoint, targetAccessKey, targetSecretKey string
	targetBucket, targetPrefix                       string
	debug                                            bool
	insecure                                         bool
)

func main() {
	flag.StringVar(&sourceEndpoint, "source-endpoint", "https://play.min.io", "S3 endpoint URL")
	flag.StringVar(&sourceAccessKey, "source-access-key", "Q3AM3UQ867SPQQA43P2F", "S3 Access Key")
	flag.StringVar(&sourceSecretKey, "source-secret-key", "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG", "S3 Secret Key")
	flag.StringVar(&sourceBucket, "source-bucket", "", "Select a specific bucket")
	flag.StringVar(&sourcePrefix, "source-prefix", "", "Select a prefix")

	flag.StringVar(&targetEndpoint, "target-endpoint", "https://play.min.io", "S3 endpoint URL")
	flag.StringVar(&targetAccessKey, "target-access-key", "Q3AM3UQ867SPQQA43P2F", "S3 Access Key")
	flag.StringVar(&targetSecretKey, "target-secret-key", "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG", "S3 Secret Key")
	flag.StringVar(&targetBucket, "target-bucket", "", "Select a specific bucket")
	flag.StringVar(&targetPrefix, "target-prefix", "", "Select a prefix")

	flag.BoolVar(&debug, "debug", false, "Prints HTTP network calls to S3 endpoint")
	flag.BoolVar(&insecure, "insecure", false, "Disable TLS verification")
	flag.Parse()

	if sourceEndpoint == "" {
		log.Fatalln("source Endpoint is not provided")
	}

	if sourceAccessKey == "" {
		log.Fatalln("source Access key is not provided")
	}

	if sourceSecretKey == "" {
		log.Fatalln("source Secret key is not provided")
	}

	if sourceBucket == "" && sourcePrefix != "" {
		log.Fatalln("--source-prefix is specified without --source-bucket.")
	}

	if targetEndpoint == "" {
		log.Fatalln("target Endpoint is not provided")
	}

	if targetAccessKey == "" {
		log.Fatalln("target Access key is not provided")
	}

	if targetSecretKey == "" {
		log.Fatalln("target Secret key is not provided")
	}

	if targetBucket == "" && targetPrefix != "" {
		log.Fatalln("--target-prefix is specified without --target-bucket.")
	}

	u, err := url.Parse(sourceEndpoint)
	if err != nil {
		log.Fatalln(err)
	}

	secure := strings.EqualFold(u.Scheme, "https")
	transport, err := minio.DefaultTransport(secure)
	if err != nil {
		log.Fatalln(err)
	}
	if insecure {
		// skip TLS verification
		transport.TLSClientConfig.InsecureSkipVerify = true
	}

	sclnt, err := minio.New(u.Host, &minio.Options{
		Creds:     credentials.NewStaticV4(sourceAccessKey, sourceSecretKey, ""),
		Secure:    secure,
		Transport: transport,
	})
	if err != nil {
		log.Fatalln(err)
	}

	tclnt, err := minio.New(u.Host, &minio.Options{
		Creds:     credentials.NewStaticV4(targetAccessKey, targetSecretKey, ""),
		Secure:    secure,
		Transport: transport,
	})
	if err != nil {
		log.Fatalln(err)
	}

	if debug {
		sclnt.TraceOn(os.Stderr)
		tclnt.TraceOn(os.Stderr)
	}

	sopts := minio.ListObjectsOptions{
		Recursive:    true,
		Prefix:       sourcePrefix,
		WithMetadata: true,
	}

	topts := minio.ListObjectsOptions{
		Recursive:    true,
		Prefix:       targetPrefix,
		WithMetadata: true,
	}

	srcCh := sclnt.ListObjects(context.Background(), sourceBucket, sopts)
	tgtCh := tclnt.ListObjects(context.Background(), targetBucket, topts)

	srcCtnt, srcOk := <-srcCh
	tgtCtnt, tgtOk := <-tgtCh

	var srcEOF, tgtEOF bool

	for {
		srcEOF = !srcOk
		tgtEOF = !tgtOk

		// No objects from source AND target: Finish
		if srcEOF && tgtEOF {
			break
		}

		if !srcEOF && srcCtnt.Err != nil {
			log.Fatal(srcCtnt.Err)
		}

		if !tgtEOF && tgtCtnt.Err != nil {
			log.Fatal(tgtCtnt.Err)
		}

		// If source doesn't have objects anymore, comparison becomes obvious
		if srcEOF {
			fmt.Printf("only in target: %s\n", tgtCtnt.Key)
			tgtCtnt, tgtOk = <-tgtCh
			continue
		}

		// The same for target
		if tgtEOF {
			fmt.Printf("only in source: %s\n", srcCtnt.Key)
			srcCtnt, srcOk = <-srcCh
			continue
		}

		if srcCtnt.Key == tgtCtnt.Key {
			var allgood bool
			if srcCtnt.Size != tgtCtnt.Size {
				fmt.Printf("differ in size sourceSize: %d, targetSize: %d\n", srcCtnt.Size, tgtCtnt.Size)
			} else if srcCtnt.ContentType != tgtCtnt.ContentType {
				fmt.Printf("differ in contentType source: %s, target: %s\n", srcCtnt.ContentType, tgtCtnt.ContentType)
			} else {
				opts := minio.GetObjectOptions{}
				sobj, err := sclnt.GetObject(context.Background(), sourceBucket, srcCtnt.Key, opts)
				if err == nil {
					tobj, err := tclnt.GetObject(context.Background(), targetBucket, tgtCtnt.Key, opts)
					if err != nil {
						fmt.Printf("unreadable on target: %s (%s)\n", tgtCtnt.Key, err)
					} else {
						srcSha256 := sha256.New()
						tgtSha256 := sha256.New()

						var sourceFailed, targetFailed bool
						var wg sync.WaitGroup

						wg.Add(2)
						go func() {
							defer wg.Done()
							srcSize, err := io.Copy(srcSha256, sobj)
							if err != nil {
								fmt.Printf("unreadable on source: %s (%s)\n", srcCtnt.Key, err)
								sourceFailed = true
								return
							}
							if srcSize != srcCtnt.Size {
								fmt.Printf("unreadable on source - size differs upon read: %s\n", srcCtnt.Key)
								sourceFailed = true
							}
						}()
						go func() {
							defer wg.Done()
							tgtSize, err := io.Copy(tgtSha256, tobj)
							if err != nil {
								fmt.Printf("unreadable on target: %s (%s)\n", tgtCtnt.Key, err)
								targetFailed = true
								return
							}
							if tgtSize != tgtCtnt.Size {
								fmt.Printf("unreadable on target - size differs upon read: %s\n", tgtCtnt.Key)
								targetFailed = true
							}
						}()
						wg.Wait()

						tobj.Close()

						if !sourceFailed && !targetFailed {
							ssum := srcSha256.Sum(nil)
							tsum := tgtSha256.Sum(nil)
							if !bytes.Equal(ssum, tsum) {
								fmt.Printf("sha256 sum mismatch: %s -> Expected(%x), Found(%x)\n", srcCtnt.Key, ssum, tsum)
							} else {
								allgood = true
							}
						}
					}

					sobj.Close()
				} else {
					fmt.Printf("unreadable on source: %s (%s)\n", srcCtnt.Key, err)
				}
			}

			if allgood {
				fmt.Printf("all readable source and target: %s -> %s\n", srcCtnt.Key, tgtCtnt.Key)
			}

			srcCtnt, srcOk = <-srcCh
			tgtCtnt, tgtOk = <-tgtCh
			continue
		}

		fmt.Printf("only in target: %s (%s)\n", tgtCtnt.Key, tgtCtnt.VersionID)
		tgtCtnt, tgtOk = <-tgtCh
	}
}
