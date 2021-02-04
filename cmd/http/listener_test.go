/*
 * MinIO Cloud Storage, (C) 2017 MinIO, Inc.
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

package http

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio-go/v7/pkg/set"
)

var serverPort uint32 = 60000

var getCert = func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	certificate, err := getTLSCert()
	if err != nil {
		return nil, err
	}
	return &certificate, nil
}

func getTLSCert() (tls.Certificate, error) {
	keyPEMBlock := []byte(`-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEApEkbPrT6wzcWK1W5atQiGptvuBsRdf8MCg4u6SN10QbslA5k
6BYRdZfFeRpwAwYyzkumug6+eBJatDZEd7+0FF86yxB7eMTSiHKRZ5Mi5ZyCFsez
dndknGBeK6I80s1jd5ZsLLuMKErvbNwSbfX+X6d2mBeYW8Scv9N+qYnNrHHHohvX
oxy1gZ18EhhogQhrD22zaqg/jtmOT8ImUiXzB1mKInt2LlSkoRYuBzepkDJrsE1L
/cyYZbtcO/ASDj+/qQAuQ66v9pNyJkIQ7bDOUyxaT5Hx9XvbqI1OqUVAdGLLi+eZ
IFguFyYd0lemwdN/IDvxftzegTO3cO0D28d1UQIDAQABAoIBAB42x8j3lerTNcOQ
h4JLM157WcedSs/NsVQkGaKM//0KbfYo04wPivR6jjngj9suh6eDKE2tqoAAuCfO
lzcCzca1YOW5yUuDv0iS8YT//XoHF7HC1pGiEaHk40zZEKCgX3u98XUkpPlAFtqJ
euY4SKkk7l24cS/ncACjj/b0PhxJoT/CncuaaJKqmCc+vdL4wj1UcrSNPZqRjDR/
sh5DO0LblB0XrqVjpNxqxM60/IkbftB8YTnyGgtO2tbTPr8KdQ8DhHQniOp+WEPV
u/iXt0LLM7u62LzadkGab2NDWS3agnmdvw2ADtv5Tt8fZ7WnPqiOpNyD5Bv1a3/h
YBw5HsUCgYEA0Sfv6BiSAFEby2KusRoq5UeUjp/SfL7vwpO1KvXeiYkPBh2XYVq2
azMnOw7Rz5ixFhtUtto2XhYdyvvr3dZu1fNHtxWo9ITBivqTGGRNwfiaQa58Bugo
gy7vCdIE/f6xE5LYIovBnES2vs/ZayMyhTX84SCWd0pTY0kdDA8ePGsCgYEAyRSA
OTzX43KUR1G/trpuM6VBc0W6YUNYzGRa1TcUxBP4K7DfKMpPGg6ulqypfoHmu8QD
L+z+iQmG9ySSuvScIW6u8LgkrTwZga8y2eb/A2FAVYY/bnelef1aMkis+bBX2OQ4
QAg2uq+pkhpW1k5NSS9lVCPkj4e5Ur9RCm9fRDMCgYAf3CSIR03eLHy+Y37WzXSh
TmELxL6sb+1Xx2Y+cAuBCda3CMTpeIb3F2ivb1d4dvrqsikaXW0Qse/B3tQUC7kA
cDmJYwxEiwBsajUD7yuFE5hzzt9nse+R5BFXfp1yD1zr7V9tC7rnUfRAZqrozgjB
D/NAW9VvwGupYRbCon7plwKBgQCRPfeoYGRoa9ji8w+Rg3QaZeGyy8jmfGjlqg9a
NyEOyIXXuThYFFmyrqw5NZpwQJBTTDApK/xnK7SLS6WY2Rr1oydFxRzo7KJX5B7M
+md1H4gCvqeOuWmThgbij1AyQsgRaDehOM2fZ0cKu2/B+Gkm1c9RSWPMsPKR7JMz
AGNFtQKBgQCRCFIdGJHnvz35vJfLoihifCejBWtZbAnZoBHpF3xMCtV755J96tUf
k1Tv9hz6WfSkOSlwLq6eGZY2dCENJRW1ft1UelpFvCjbfrfLvoFFLs3gu0lfqXHi
CS6fjhn9Ahvz10yD6fd4ixRUjoJvULzI0Sxc1O95SYVF1lIAuVr9Hw==
-----END RSA PRIVATE KEY-----`)
	certPEMBlock := []byte(`-----BEGIN CERTIFICATE-----
MIIDXTCCAkWgAwIBAgIJAKlqK5HKlo9MMA0GCSqGSIb3DQEBCwUAMEUxCzAJBgNV
BAYTAkFVMRMwEQYDVQQIDApTb21lLVN0YXRlMSEwHwYDVQQKDBhJbnRlcm5ldCBX
aWRnaXRzIFB0eSBMdGQwHhcNMTcwNjE5MTA0MzEyWhcNMjcwNjE3MTA0MzEyWjBF
MQswCQYDVQQGEwJBVTETMBEGA1UECAwKU29tZS1TdGF0ZTEhMB8GA1UECgwYSW50
ZXJuZXQgV2lkZ2l0cyBQdHkgTHRkMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIB
CgKCAQEApEkbPrT6wzcWK1W5atQiGptvuBsRdf8MCg4u6SN10QbslA5k6BYRdZfF
eRpwAwYyzkumug6+eBJatDZEd7+0FF86yxB7eMTSiHKRZ5Mi5ZyCFsezdndknGBe
K6I80s1jd5ZsLLuMKErvbNwSbfX+X6d2mBeYW8Scv9N+qYnNrHHHohvXoxy1gZ18
EhhogQhrD22zaqg/jtmOT8ImUiXzB1mKInt2LlSkoRYuBzepkDJrsE1L/cyYZbtc
O/ASDj+/qQAuQ66v9pNyJkIQ7bDOUyxaT5Hx9XvbqI1OqUVAdGLLi+eZIFguFyYd
0lemwdN/IDvxftzegTO3cO0D28d1UQIDAQABo1AwTjAdBgNVHQ4EFgQUqMVdMIA1
68Dv+iwGugAaEGUSd0IwHwYDVR0jBBgwFoAUqMVdMIA168Dv+iwGugAaEGUSd0Iw
DAYDVR0TBAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEAjQVoqRv2HlE5PJIX/qk5
oMOKZlHTyJP+s2HzOOVt+eCE/jNdfC7+8R/HcPldQs7p9GqH2F6hQ9aOtDhJVEaU
pjxCi4qKeZ1kWwqv8UMBXW92eHGysBvE2Gmm/B1JFl8S2GR5fBmheZVnYW893MoI
gp+bOoCcIuMJRqCra4vJgrOsQjgRElQvd2OlP8qQzInf/fRqO/AnZPwMkGr3+KZ0
BKEOXtmSZaPs3xEsnvJd8wrTgA0NQK7v48E+gHSXzQtaHmOLqisRXlUOu2r1gNCJ
rr3DRiUP6V/10CZ/ImeSJ72k69VuTw9vq2HzB4x6pqxF2X7JQSLUCS2wfNN13N0d
9A==
-----END CERTIFICATE-----`)

	return tls.X509KeyPair(certPEMBlock, keyPEMBlock)
}

func getNextPort() string {
	return strconv.Itoa(int(atomic.AddUint32(&serverPort, 1)))
}

func getNonLoopBackIP(t *testing.T) string {
	localIP4 := set.NewStringSet()
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		t.Fatalf("%s.  Unable to get IP addresses of this host.", err)
	}

	for _, addr := range addrs {
		var ip net.IP
		switch v := addr.(type) {
		case *net.IPNet:
			ip = v.IP
		case *net.IPAddr:
			ip = v.IP
		}

		if ip.To4() != nil {
			localIP4.Add(ip.String())
		}
	}

	// Filter ipList by IPs those do not start with '127.'.
	nonLoopBackIPs := localIP4.FuncMatch(func(ip string, matchString string) bool {
		return !strings.HasPrefix(ip, "127.")
	}, "")
	if len(nonLoopBackIPs) == 0 {
		t.Fatalf("No non-loop back IP address found for this host")
	}
	nonLoopBackIP := nonLoopBackIPs.ToSlice()[0]
	return nonLoopBackIP
}

func TestNewHTTPListener(t *testing.T) {
	testCases := []struct {
		serverAddrs         []string
		tcpKeepAliveTimeout time.Duration
		readTimeout         time.Duration
		writeTimeout        time.Duration
		expectedErr         bool
	}{
		{[]string{"93.184.216.34:65432"}, time.Duration(0), time.Duration(0), time.Duration(0), true},
		{[]string{"example.org:65432"}, time.Duration(0), time.Duration(0), time.Duration(0), true},
		{[]string{"unknown-host"}, time.Duration(0), time.Duration(0), time.Duration(0), true},
		{[]string{"unknown-host:65432"}, time.Duration(0), time.Duration(0), time.Duration(0), true},
		{[]string{"localhost:65432", "93.184.216.34:65432"}, time.Duration(0), time.Duration(0), time.Duration(0), true},
		{[]string{"localhost:65432", "unknown-host:65432"}, time.Duration(0), time.Duration(0), time.Duration(0), true},
		{[]string{"localhost:0"}, time.Duration(0), time.Duration(0), time.Duration(0), false},
		{[]string{"localhost:0"}, time.Duration(0), time.Duration(0), time.Duration(0), false},
	}

	for _, testCase := range testCases {
		listener, err := newHTTPListener(
			testCase.serverAddrs,
		)

		if !testCase.expectedErr {
			if err != nil {
				t.Fatalf("error: expected = <nil>, got = %v", err)
			}
		} else if err == nil {
			t.Fatalf("error: expected = %v, got = <nil>", testCase.expectedErr)
		}

		if err == nil {
			listener.Close()
		}
	}
}

func TestHTTPListenerStartClose(t *testing.T) {
	nonLoopBackIP := getNonLoopBackIP(t)

	testCases := []struct {
		serverAddrs []string
	}{
		{[]string{"localhost:0"}},
		{[]string{nonLoopBackIP + ":0"}},
		{[]string{"127.0.0.1:0", nonLoopBackIP + ":0"}},
		{[]string{"localhost:0"}},
		{[]string{nonLoopBackIP + ":0"}},
		{[]string{"127.0.0.1:0", nonLoopBackIP + ":0"}},
	}

	for i, testCase := range testCases {
		listener, err := newHTTPListener(
			testCase.serverAddrs,
		)
		if err != nil {
			if strings.Contains(err.Error(), "The requested address is not valid in its context") {
				// Ignore if IP is unbindable.
				continue
			}
			t.Fatalf("Test %d: error: expected = <nil>, got = %v", i+1, err)
		}

		for _, serverAddr := range listener.Addrs() {
			conn, err := net.Dial("tcp", serverAddr.String())
			if err != nil {
				t.Fatalf("Test %d: error: expected = <nil>, got = %v", i+1, err)
			}
			conn.Close()
		}

		listener.Close()
	}
}

func TestHTTPListenerAddr(t *testing.T) {
	nonLoopBackIP := getNonLoopBackIP(t)
	var casePorts []string
	for i := 0; i < 6; i++ {
		casePorts = append(casePorts, getNextPort())
	}

	testCases := []struct {
		serverAddrs  []string
		expectedAddr string
	}{
		{[]string{"localhost:" + casePorts[0]}, "127.0.0.1:" + casePorts[0]},
		{[]string{nonLoopBackIP + ":" + casePorts[1]}, nonLoopBackIP + ":" + casePorts[1]},
		{[]string{"127.0.0.1:" + casePorts[2], nonLoopBackIP + ":" + casePorts[2]}, "0.0.0.0:" + casePorts[2]},
		{[]string{"localhost:" + casePorts[3]}, "127.0.0.1:" + casePorts[3]},
		{[]string{nonLoopBackIP + ":" + casePorts[4]}, nonLoopBackIP + ":" + casePorts[4]},
		{[]string{"127.0.0.1:" + casePorts[5], nonLoopBackIP + ":" + casePorts[5]}, "0.0.0.0:" + casePorts[5]},
	}

	for i, testCase := range testCases {
		listener, err := newHTTPListener(
			testCase.serverAddrs,
		)
		if err != nil {
			if strings.Contains(err.Error(), "The requested address is not valid in its context") {
				// Ignore if IP is unbindable.
				continue
			}
			t.Fatalf("Test %d: error: expected = <nil>, got = %v", i+1, err)
		}

		addr := listener.Addr()
		if addr.String() != testCase.expectedAddr {
			t.Fatalf("Test %d: addr: expected = %v, got = %v", i+1, testCase.expectedAddr, addr)
		}

		listener.Close()
	}
}

func TestHTTPListenerAddrs(t *testing.T) {
	nonLoopBackIP := getNonLoopBackIP(t)
	var casePorts []string
	for i := 0; i < 6; i++ {
		casePorts = append(casePorts, getNextPort())
	}

	testCases := []struct {
		serverAddrs   []string
		expectedAddrs set.StringSet
	}{
		{[]string{"localhost:" + casePorts[0]}, set.CreateStringSet("127.0.0.1:" + casePorts[0])},
		{[]string{nonLoopBackIP + ":" + casePorts[1]}, set.CreateStringSet(nonLoopBackIP + ":" + casePorts[1])},
		{[]string{"127.0.0.1:" + casePorts[2], nonLoopBackIP + ":" + casePorts[2]}, set.CreateStringSet("127.0.0.1:"+casePorts[2], nonLoopBackIP+":"+casePorts[2])},
		{[]string{"localhost:" + casePorts[3]}, set.CreateStringSet("127.0.0.1:" + casePorts[3])},
		{[]string{nonLoopBackIP + ":" + casePorts[4]}, set.CreateStringSet(nonLoopBackIP + ":" + casePorts[4])},
		{[]string{"127.0.0.1:" + casePorts[5], nonLoopBackIP + ":" + casePorts[5]}, set.CreateStringSet("127.0.0.1:"+casePorts[5], nonLoopBackIP+":"+casePorts[5])},
	}

	for i, testCase := range testCases {
		listener, err := newHTTPListener(
			testCase.serverAddrs,
		)
		if err != nil {
			if strings.Contains(err.Error(), "The requested address is not valid in its context") {
				// Ignore if IP is unbindable.
				continue
			}
			t.Fatalf("Test %d: error: expected = <nil>, got = %v", i+1, err)
		}

		addrs := listener.Addrs()
		addrSet := set.NewStringSet()
		for _, addr := range addrs {
			addrSet.Add(addr.String())
		}

		if !addrSet.Equals(testCase.expectedAddrs) {
			t.Fatalf("Test %d: addr: expected = %v, got = %v", i+1, testCase.expectedAddrs, addrs)
		}

		listener.Close()
	}
}

type myTimeoutErr struct {
	timeout bool
}

func (m *myTimeoutErr) Error() string { return fmt.Sprintf("myTimeoutErr: %v", m.timeout) }
func (m *myTimeoutErr) Timeout() bool { return m.timeout }

// Test for ignoreErr helper function
func TestIgnoreErr(t *testing.T) {
	testCases := []struct {
		err  error
		want bool
	}{
		{
			err:  io.EOF,
			want: true,
		},
		{
			err:  &net.OpError{Err: &myTimeoutErr{timeout: true}},
			want: true,
		},
		{
			err:  errors.New("EOF"),
			want: true,
		},
		{
			err:  &net.OpError{Err: &myTimeoutErr{timeout: false}},
			want: false,
		},
		{
			err:  io.ErrUnexpectedEOF,
			want: false,
		},
		{
			err:  nil,
			want: false,
		},
	}

	for i, tc := range testCases {
		if actual := isRoutineNetErr(tc.err); actual != tc.want {
			t.Errorf("Test case %d: Expected %v but got %v for %v", i+1,
				tc.want, actual, tc.err)
		}
	}
}
