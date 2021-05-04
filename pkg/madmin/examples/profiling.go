// +build ignore

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
	"io"
	"log"
	"os"
	"time"

	"github.com/minio/minio/pkg/madmin"
)

func main() {
	// Note: YOUR-ACCESSKEYID, YOUR-SECRETACCESSKEY are
	// dummy values, please replace them with original values.

	// Note: YOUR-ACCESSKEYID, YOUR-SECRETACCESSKEY are
	// dummy values, please replace them with original values.

	// API requests are secure (HTTPS) if secure=true and insecure (HTTP) otherwise.
	// New returns an MinIO Admin client object.
	madmClnt, err := madmin.New("your-minio.example.com:9000", "YOUR-ACCESSKEYID", "YOUR-SECRETACCESSKEY", true)
	if err != nil {
		log.Fatalln(err)
	}

	profiler := madmin.ProfilerCPU
	log.Println("Starting " + profiler + " profiling..")

	startResults, err := madmClnt.StartProfiling(context.Background(), profiler)
	if err != nil {
		log.Fatalln(err)
	}

	for _, result := range startResults {
		if !result.Success {
			log.Printf("Unable to start profiling on node `%s`, reason = `%s`\n", result.NodeName, result.Error)
			continue
		}
		log.Printf("Profiling successfully started on node `%s`\n", result.NodeName)
	}

	sleep := time.Duration(10)
	time.Sleep(time.Second * sleep)

	log.Println("Stopping profiling..")

	profilingData, err := madmClnt.DownloadProfilingData(context.Background())
	if err != nil {
		log.Fatalln(err)
	}

	profilingFile, err := os.Create("/tmp/profiling-" + string(profiler) + ".zip")
	if err != nil {
		log.Fatal(err)
	}

	if _, err := io.Copy(profilingFile, profilingData); err != nil {
		log.Fatal(err)
	}

	if err := profilingFile.Close(); err != nil {
		log.Fatal(err)
	}

	if err := profilingData.Close(); err != nil {
		log.Fatal(err)
	}

	log.Println("Profiling files " + profilingFile.Name() + " successfully downloaded.")
}
