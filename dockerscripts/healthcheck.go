/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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

package main

import (
	"bufio"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"time"

	xhttp "github.com/minio/minio/cmd/http"
)

const (
	initGraceTime = 300
	healthPath    = "/minio/health/live"
	timeout       = time.Duration(30 * time.Second)
	minioProcess  = "minio"
)

// returns container boot time by finding
// modtime of /proc/1 directory
func getStartTime() time.Time {
	di, err := os.Stat("/proc/1")
	if err != nil {
		// Cant stat proc dir successfully, exit with error
		log.Fatalln(err)
	}
	return di.ModTime()
}

// Returns the ip:port of the MinIO process
// running in the container
func findEndpoint() (string, error) {
	cmd := exec.Command("netstat", "-ntlp")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		// error getting stdout pipe
		return "", err
	}
	if err = cmd.Start(); err != nil {
		// error executing the command.
		return "", err
	}
	// split netstat output in rows
	scanner := bufio.NewScanner(stdout)
	scanner.Split(bufio.ScanLines)
	// loop over the rows to find MinIO process
	for scanner.Scan() {
		if strings.Contains(scanner.Text(), minioProcess) {
			line := scanner.Text()
			newLine := strings.Replace(line, ":::", "127.0.0.1:", 1)
			fields := strings.Fields(newLine)
			// index 3 in the row has the Local address
			// find the last index of ":" - address will
			// have port number after this index
			i := strings.LastIndex(fields[3], ":")
			// split address and port
			addr := fields[3][:i]
			port := fields[3][i+1:]
			// add surrounding [] for ip6 address
			if strings.Count(addr, ":") > 0 {
				addr = strings.Join([]string{"[", addr, "]"}, "")
			}
			// wait for cmd to complete before return
			if err = cmd.Wait(); err != nil {
				return "", err
			}
			// return joint address and port
			return strings.Join([]string{addr, port}, ":"), nil
		}
	}
	if err = scanner.Err(); err != nil {
		return "", err
	}
	if err = cmd.Wait(); err != nil {
		// command failed to run
		return "", err
	}
	// minio process not found, exit with error
	return "", errors.New("no minio process found")
}

func main() {
	startTime := getStartTime()

	//  In distributed environment like Swarm, traffic is routed
	//  to a container only when it reports a `healthy` status. So, we exit
	//  with 0 to ensure healthy status till distributed MinIO starts (120s).

	//  Refer: https://github.com/moby/moby/pull/28938#issuecomment-301753272

	if (time.Now().Sub(startTime) / time.Second) > initGraceTime {
		endPoint, err := findEndpoint()
		if err != nil {
			log.Fatalln(err)
		}
		u, err := url.Parse(fmt.Sprintf("http://%s%s", endPoint, healthPath))
		if err != nil {
			// Could not parse URL successfully
			log.Fatalln(err)
		}
		// MinIO server may be using self-signed or CA certificates. To avoid
		// making Docker setup complicated, we skip verifying certificates here.
		// This is because, following request tests for health status within
		// containerized environment, i.e. requests are always made to the MinIO
		// server running on the same host.
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
		client := &http.Client{Transport: tr, Timeout: timeout}
		resp, err := client.Get(u.String())
		if err != nil {
			// GET failed exit
			log.Fatalln(err)
		}
		if resp.StatusCode == http.StatusOK {
			// Drain any response.
			xhttp.DrainBody(resp.Body)
			// exit with success
			os.Exit(0)
		}
		// Drain any response.
		xhttp.DrainBody(resp.Body)
		// 400 response may mean sever is configured with https
		if resp.StatusCode == http.StatusBadRequest {
			// Try with https
			u.Scheme = "https"
			resp, err = client.Get(u.String())
			if err != nil {
				// GET failed exit
				log.Fatalln(err)
			}
			if resp.StatusCode == http.StatusOK {
				// Drain any response.
				xhttp.DrainBody(resp.Body)
				// exit with success
				os.Exit(0)
			}
			// Drain any response.
			xhttp.DrainBody(resp.Body)
		}
		// Execution reaching here means none of
		// the success cases were satisfied
		os.Exit(1)
	}
	os.Exit(0)
}
