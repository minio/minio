/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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
	"bufio"
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio-go/pkg/set"
)

var serverPort uint32 = 60000

// fail - as t.Fatalf() is not goroutine safe, this function behaves like t.Fatalf().
func fail(t *testing.T, template string, args ...interface{}) {
	fmt.Printf(template, args...)
	fmt.Println()
	t.Fail()
}

func getNextPort() string {
	return strconv.Itoa(int(atomic.AddUint32(&serverPort, 1)))
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

func getTLSConfig(t *testing.T) *tls.Config {
	tlsCert, err := getTLSCert()
	if err != nil {
		t.Fatalf("Unable to parse private/certificate data. %v\n", err)
	}
	tlsConfig := &tls.Config{
		PreferServerCipherSuites: true,
		MinVersion:               tls.VersionTLS12,
		NextProtos:               []string{"http/1.1", "h2"},
	}
	tlsConfig.Certificates = append(tlsConfig.Certificates, tlsCert)

	return tlsConfig
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

// Test getMethodMaxLen()
func TestGetMethodMaxLen(t *testing.T) {
	l := getMethodMaxLen()
	if l != (methodMaxLen - 1) {
		t.Fatalf("expected: %v, got: %v", (methodMaxLen - 1), l)
	}
}

// Test isHTTPMethod()
func TestIsHTTPMethod(t *testing.T) {
	testCases := []struct {
		method         string
		expectedResult bool
	}{
		{"", false},
		{"get", false},
		{"put", false},
		{"UPLOAD", false},
		{"OPTIONS", true},
		{"GET", true},
		{"HEAD", true},
		{"POST", true},
		{"PUT", true},
		{"DELETE", true},
		{"TRACE", true},
		{"CONNECT", true},
		{"PRI", true},
	}

	for _, testCase := range testCases {
		result := isHTTPMethod(testCase.method)
		if result != testCase.expectedResult {
			t.Fatalf("expected: %v, got: %v", testCase.expectedResult, result)
		}
	}
}

func TestNewHTTPListener(t *testing.T) {
	errMsg := ": no such host"

	remoteAddrErrMsg := "listen tcp 93.184.216.34:65432: bind: cannot assign requested address"
	if runtime.GOOS == "windows" {
		remoteAddrErrMsg = "listen tcp 93.184.216.34:65432: bind: The requested address is not valid in its context."
	} else if runtime.GOOS == "darwin" {
		remoteAddrErrMsg = "listen tcp 93.184.216.34:65432: bind: can't assign requested address"
	}

	tlsConfig := getTLSConfig(t)

	testCases := []struct {
		serverAddrs            []string
		tlsConfig              *tls.Config
		tcpKeepAliveTimeout    time.Duration
		readTimeout            time.Duration
		writeTimeout           time.Duration
		updateBytesReadFunc    func(int)
		updateBytesWrittenFunc func(int)
		errorLogFunc           func(error, string, ...interface{})
		expectedErr            error
	}{
		{[]string{"93.184.216.34:65432"}, nil, time.Duration(0), time.Duration(0), time.Duration(0), nil, nil, nil, errors.New(remoteAddrErrMsg)},
		{[]string{"example.org:65432"}, nil, time.Duration(0), time.Duration(0), time.Duration(0), nil, nil, nil, errors.New(remoteAddrErrMsg)},
		{[]string{"unknown-host"}, nil, time.Duration(0), time.Duration(0), time.Duration(0), nil, nil, nil, errors.New("listen tcp: address unknown-host: missing port in address")},
		{[]string{"unknown-host:65432"}, nil, time.Duration(0), time.Duration(0), time.Duration(0), nil, nil, nil, errors.New("listen tcp: lookup unknown-host" + errMsg)},
		{[]string{"localhost:65432", "93.184.216.34:65432"}, nil, time.Duration(0), time.Duration(0), time.Duration(0), nil, nil, nil, errors.New(remoteAddrErrMsg)},
		{[]string{"localhost:65432", "unknown-host:65432"}, nil, time.Duration(0), time.Duration(0), time.Duration(0), nil, nil, nil, errors.New("listen tcp: lookup unknown-host" + errMsg)},
		{[]string{"localhost:0"}, nil, time.Duration(0), time.Duration(0), time.Duration(0), nil, nil, nil, nil},
		{[]string{"localhost:0"}, tlsConfig, time.Duration(0), time.Duration(0), time.Duration(0), nil, nil, nil, nil},
	}

	for _, testCase := range testCases {
		listener, err := newHTTPListener(
			testCase.serverAddrs,
			testCase.tlsConfig,
			testCase.tcpKeepAliveTimeout,
			testCase.readTimeout,
			testCase.writeTimeout,
			testCase.updateBytesReadFunc,
			testCase.updateBytesWrittenFunc,
			testCase.errorLogFunc,
		)

		if testCase.expectedErr == nil {
			if err != nil {
				t.Fatalf("error: expected = <nil>, got = %v", err)
			}
		} else if err == nil {
			t.Fatalf("error: expected = %v, got = <nil>", testCase.expectedErr)
		} else {
			var match bool
			if strings.HasSuffix(testCase.expectedErr.Error(), errMsg) {
				match = strings.HasSuffix(err.Error(), errMsg)
			} else {
				match = (testCase.expectedErr.Error() == err.Error())
			}
			if !match {
				t.Fatalf("error: expected = %v, got = %v", testCase.expectedErr, err)
			}
		}

		if err == nil {
			listener.Close()
		}
	}
}

func TestHTTPListenerStartClose(t *testing.T) {
	tlsConfig := getTLSConfig(t)
	nonLoopBackIP := getNonLoopBackIP(t)

	testCases := []struct {
		serverAddrs []string
		tlsConfig   *tls.Config
	}{
		{[]string{"localhost:0"}, nil},
		{[]string{nonLoopBackIP + ":0"}, nil},
		{[]string{"127.0.0.1:0", nonLoopBackIP + ":0"}, nil},
		{[]string{"localhost:0"}, tlsConfig},
		{[]string{nonLoopBackIP + ":0"}, tlsConfig},
		{[]string{"127.0.0.1:0", nonLoopBackIP + ":0"}, tlsConfig},
	}

	for i, testCase := range testCases {
		listener, err := newHTTPListener(
			testCase.serverAddrs,
			testCase.tlsConfig,
			time.Duration(0),
			time.Duration(0),
			time.Duration(0),
			nil,
			nil,
			nil,
		)
		if err != nil {
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
	tlsConfig := getTLSConfig(t)
	nonLoopBackIP := getNonLoopBackIP(t)
	var casePorts []string
	for i := 0; i < 6; i++ {
		casePorts = append(casePorts, getNextPort())
	}

	testCases := []struct {
		serverAddrs  []string
		tlsConfig    *tls.Config
		expectedAddr string
	}{
		{[]string{"localhost:" + casePorts[0]}, nil, "127.0.0.1:" + casePorts[0]},
		{[]string{nonLoopBackIP + ":" + casePorts[1]}, nil, nonLoopBackIP + ":" + casePorts[1]},
		{[]string{"127.0.0.1:" + casePorts[2], nonLoopBackIP + ":" + casePorts[2]}, nil, "0.0.0.0:" + casePorts[2]},
		{[]string{"localhost:" + casePorts[3]}, tlsConfig, "127.0.0.1:" + casePorts[3]},
		{[]string{nonLoopBackIP + ":" + casePorts[4]}, tlsConfig, nonLoopBackIP + ":" + casePorts[4]},
		{[]string{"127.0.0.1:" + casePorts[5], nonLoopBackIP + ":" + casePorts[5]}, tlsConfig, "0.0.0.0:" + casePorts[5]},
	}

	for i, testCase := range testCases {
		listener, err := newHTTPListener(
			testCase.serverAddrs,
			testCase.tlsConfig,
			time.Duration(0),
			time.Duration(0),
			time.Duration(0),
			nil,
			nil,
			nil,
		)
		if err != nil {
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
	tlsConfig := getTLSConfig(t)
	nonLoopBackIP := getNonLoopBackIP(t)
	var casePorts []string
	for i := 0; i < 6; i++ {
		casePorts = append(casePorts, getNextPort())
	}

	testCases := []struct {
		serverAddrs   []string
		tlsConfig     *tls.Config
		expectedAddrs set.StringSet
	}{
		{[]string{"localhost:" + casePorts[0]}, nil, set.CreateStringSet("127.0.0.1:" + casePorts[0])},
		{[]string{nonLoopBackIP + ":" + casePorts[1]}, nil, set.CreateStringSet(nonLoopBackIP + ":" + casePorts[1])},
		{[]string{"127.0.0.1:" + casePorts[2], nonLoopBackIP + ":" + casePorts[2]}, nil, set.CreateStringSet("127.0.0.1:"+casePorts[2], nonLoopBackIP+":"+casePorts[2])},
		{[]string{"localhost:" + casePorts[3]}, tlsConfig, set.CreateStringSet("127.0.0.1:" + casePorts[3])},
		{[]string{nonLoopBackIP + ":" + casePorts[4]}, tlsConfig, set.CreateStringSet(nonLoopBackIP + ":" + casePorts[4])},
		{[]string{"127.0.0.1:" + casePorts[5], nonLoopBackIP + ":" + casePorts[5]}, tlsConfig, set.CreateStringSet("127.0.0.1:"+casePorts[5], nonLoopBackIP+":"+casePorts[5])},
	}

	for i, testCase := range testCases {
		listener, err := newHTTPListener(
			testCase.serverAddrs,
			testCase.tlsConfig,
			time.Duration(0),
			time.Duration(0),
			time.Duration(0),
			nil,
			nil,
			nil,
		)
		if err != nil {
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

func TestHTTPListenerAccept(t *testing.T) {
	tlsConfig := getTLSConfig(t)
	nonLoopBackIP := getNonLoopBackIP(t)

	testCases := []struct {
		serverAddrs []string
		tlsConfig   *tls.Config
		request     string
		reply       string
	}{
		{[]string{"localhost:0"}, nil, "GET / HTTP/1.0\n", "200 OK\n"},
		{[]string{nonLoopBackIP + ":0"}, nil, "POST / HTTP/1.0\n", "200 OK\n"},
		{[]string{"127.0.0.1:0", nonLoopBackIP + ":0"}, nil, "CONNECT \n", "200 OK\n"},
		{[]string{"localhost:0"}, tlsConfig, "GET / HTTP/1.0\n", "200 OK\n"},
		{[]string{nonLoopBackIP + ":0"}, tlsConfig, "POST / HTTP/1.0\n", "200 OK\n"},
		{[]string{"127.0.0.1:0", nonLoopBackIP + ":0"}, tlsConfig, "CONNECT \n", "200 OK\n"},
	}

	for i, testCase := range testCases {
		listener, err := newHTTPListener(
			testCase.serverAddrs,
			testCase.tlsConfig,
			time.Duration(0),
			time.Duration(0),
			time.Duration(0),
			nil,
			nil,
			nil,
		)
		if err != nil {
			t.Fatalf("Test %d: error: expected = <nil>, got = %v", i+1, err)
		}

		for _, serverAddr := range listener.Addrs() {
			var conn net.Conn
			var err error

			if testCase.tlsConfig == nil {
				conn, err = net.Dial("tcp", serverAddr.String())
			} else {
				conn, err = tls.Dial("tcp", serverAddr.String(), &tls.Config{InsecureSkipVerify: true})
			}
			if err != nil {
				t.Fatalf("Test %d: error: expected = <nil>, got = %v", i+1, err)
			}

			if _, err = io.WriteString(conn, testCase.request); err != nil {
				t.Fatalf("Test %d: request send: expected = <nil>, got = %v", i+1, err)
			}

			serverConn, err := listener.Accept()
			if err != nil {
				t.Fatalf("Test %d: accept: expected = <nil>, got = %v", i+1, err)
			}

			request, err := bufio.NewReader(serverConn).ReadString('\n')
			if err != nil {
				t.Fatalf("Test %d: request read: expected = <nil>, got = %v", i+1, err)
			}

			if testCase.request != request {
				t.Fatalf("Test %d: request: expected = %v, got = %v", i+1, testCase.request, request)
			}

			if _, err = io.WriteString(serverConn, testCase.reply); err != nil {
				t.Fatalf("Test %d: reply send: expected = <nil>, got = %v", i+1, err)
			}

			reply, err := bufio.NewReader(conn).ReadString('\n')
			if err != nil {
				t.Fatalf("Test %d: reply read: expected = <nil>, got = %v", i+1, err)
			}

			if testCase.reply != reply {
				t.Fatalf("Test %d: reply: expected = %v, got = %v", i+1, testCase.reply, reply)
			}

			serverConn.Close()
			conn.Close()
		}

		listener.Close()
	}
}

func TestHTTPListenerAcceptPeekError(t *testing.T) {
	tlsConfig := getTLSConfig(t)
	nonLoopBackIP := getNonLoopBackIP(t)
	errorFunc := func(err error, template string, args ...interface{}) {
		msg := fmt.Sprintf("error: %v.  ", err)
		msg += fmt.Sprintf(template, args...)
		fmt.Println(msg)
	}

	testCases := []struct {
		serverAddrs []string
		tlsConfig   *tls.Config
		request     string
	}{
		{[]string{"127.0.0.1:0", nonLoopBackIP + ":0"}, nil, "CONN"},
		{[]string{"127.0.0.1:0", nonLoopBackIP + ":0"}, tlsConfig, "CONN"},
	}

	for i, testCase := range testCases {
		listener, err := newHTTPListener(
			testCase.serverAddrs,
			testCase.tlsConfig,
			time.Duration(0),
			time.Duration(0),
			time.Duration(0),
			nil,
			nil,
			errorFunc,
		)
		if err != nil {
			t.Fatalf("Test %d: error: expected = <nil>, got = %v", i+1, err)
		}

		go func() {
			serverConn, aerr := listener.Accept()
			if aerr == nil {
				fail(t, "Test %d: accept: expected = <error>, got = <nil>", i+1)
			}
			if serverConn != nil {
				fail(t, "Test %d: accept: server expected = <nil>, got = %v", i+1, serverConn)
			}
		}()

		for _, serverAddr := range listener.Addrs() {
			conn, err := net.Dial("tcp", serverAddr.String())
			if err != nil {
				t.Fatalf("Test %d: error: expected = <nil>, got = %v", i+1, err)
			}

			if _, err = io.WriteString(conn, testCase.request); err != nil {
				t.Fatalf("Test %d: request send: expected = <nil>, got = %v", i+1, err)
			}

			conn.Close()
		}

		listener.Close()
	}
}

func TestHTTPListenerAcceptTLSError(t *testing.T) {
	tlsConfig := getTLSConfig(t)
	nonLoopBackIP := getNonLoopBackIP(t)
	errorFunc := func(err error, template string, args ...interface{}) {
		msg := fmt.Sprintf("error: %v.  ", err)
		msg += fmt.Sprintf(template, args...)
		fmt.Println(msg)
	}

	testCases := []struct {
		serverAddrs []string
		tlsConfig   *tls.Config
		request     string
	}{
		{[]string{"127.0.0.1:0", nonLoopBackIP + ":0"}, tlsConfig, "GET / HTTP/1.0\n"},
	}

	for i, testCase := range testCases {
		listener, err := newHTTPListener(
			testCase.serverAddrs,
			testCase.tlsConfig,
			time.Duration(0),
			time.Duration(0),
			time.Duration(0),
			nil,
			nil,
			errorFunc,
		)
		if err != nil {
			t.Fatalf("Test %d: error: expected = <nil>, got = %v", i+1, err)
		}

		for _, serverAddr := range listener.Addrs() {
			conn, err := net.Dial("tcp", serverAddr.String())
			if err != nil {
				t.Fatalf("Test %d: error: expected = <nil>, got = %v", i+1, err)
			}

			if _, err = io.WriteString(conn, testCase.request); err != nil {
				t.Fatalf("Test %d: request send: expected = <nil>, got = %v", i+1, err)
			}

			go func() {
				serverConn, aerr := listener.Accept()
				if aerr == nil {
					fail(t, "Test %d: accept: expected = <error>, got = <nil>", i+1)
				}
				if serverConn != nil {
					fail(t, "Test %d: accept: server expected = <nil>, got = %v", i+1, serverConn)
				}
			}()

			buf := make([]byte, len(sslRequiredErrMsg))
			n, err := io.ReadFull(conn, buf)
			if err != nil {
				t.Fatalf("Test %d: reply read: expected = <nil> got = %v", i+1, err)
			} else if n != len(buf) {
				t.Fatalf("Test %d: reply length: expected = %v got = %v", i+1, len(buf), n)
			} else if !bytes.Equal(buf, sslRequiredErrMsg) {
				t.Fatalf("Test %d: reply: expected = %v got = %v", i+1, string(sslRequiredErrMsg), string(buf))
			}

			conn.Close()
		}

		listener.Close()
	}
}

func TestHTTPListenerAcceptError(t *testing.T) {
	tlsConfig := getTLSConfig(t)
	nonLoopBackIP := getNonLoopBackIP(t)
	errorFunc := func(err error, template string, args ...interface{}) {
		msg := fmt.Sprintf("error: %v.  ", err)
		msg += fmt.Sprintf(template, args...)
		fmt.Println(msg)
	}

	testCases := []struct {
		serverAddrs  []string
		tlsConfig    *tls.Config
		secureClient bool
		request      string
	}{
		{[]string{"127.0.0.1:0", nonLoopBackIP + ":0"}, nil, false, "CONNECTION"},
		{[]string{"127.0.0.1:0", nonLoopBackIP + ":0"}, tlsConfig, false, "CONNECTION"},
		{[]string{"127.0.0.1:0", nonLoopBackIP + ":0"}, tlsConfig, true, "CONNECTION"},
	}

	for i, testCase := range testCases {
		listener, err := newHTTPListener(
			testCase.serverAddrs,
			testCase.tlsConfig,
			time.Duration(0),
			time.Duration(0),
			time.Duration(0),
			nil,
			nil,
			errorFunc,
		)
		if err != nil {
			t.Fatalf("Test %d: error: expected = <nil>, got = %v", i+1, err)
		}

		for _, serverAddr := range listener.Addrs() {
			var conn net.Conn
			var err error

			if testCase.secureClient {
				conn, err = tls.Dial("tcp", serverAddr.String(), &tls.Config{InsecureSkipVerify: true})
			} else {
				conn, err = net.Dial("tcp", serverAddr.String())
			}
			if err != nil {
				t.Fatalf("Test %d: error: expected = <nil>, got = %v", i+1, err)
			}

			if _, err = io.WriteString(conn, testCase.request); err != nil {
				t.Fatalf("Test %d: request send: expected = <nil>, got = %v", i+1, err)
			}

			go func() {
				serverConn, aerr := listener.Accept()
				if aerr == nil {
					fail(t, "Test %d: accept: expected = <error>, got = <nil>", i+1)
				}
				if serverConn != nil {
					fail(t, "Test %d: accept: server expected = <nil>, got = %v", i+1, serverConn)
				}
			}()

			_, err = bufio.NewReader(conn).ReadString('\n')
			if err == nil {
				t.Fatalf("Test %d: reply read: expected = EOF got = <nil>", i+1)
			} else if err.Error() != "EOF" {
				t.Fatalf("Test %d: reply read: expected = EOF got = %v", i+1, err)
			}

			conn.Close()
		}

		listener.Close()
	}
}

func TestHTTPListenerAcceptParallel(t *testing.T) {
	tlsConfig := getTLSConfig(t)
	nonLoopBackIP := getNonLoopBackIP(t)

	testCases := []struct {
		serverAddrs []string
		tlsConfig   *tls.Config
		reply       string
	}{
		{[]string{"127.0.0.1:0", nonLoopBackIP + ":0"}, nil, "200 OK\n"},
		{[]string{"127.0.0.1:0", nonLoopBackIP + ":0"}, tlsConfig, "200 OK\n"},
	}

	connect := func(i int, serverAddr string, secure bool, delay bool, request, reply string) {
		var conn net.Conn
		var err error

		if secure {
			conn, err = tls.Dial("tcp", serverAddr, &tls.Config{InsecureSkipVerify: true})
		} else {
			conn, err = net.Dial("tcp", serverAddr)
		}
		if err != nil {
			fail(t, "Test %d: error: expected = <nil>, got = %v", i+1, err)
		}

		if delay {
			if _, err = io.WriteString(conn, request[:3]); err != nil {
				fail(t, "Test %d: request send: expected = <nil>, got = %v", i+1, err)
			}
			time.Sleep(1 * time.Second)
			if _, err = io.WriteString(conn, request[3:]); err != nil {
				fail(t, "Test %d: request send: expected = <nil>, got = %v", i+1, err)
			}
		} else {
			if _, err = io.WriteString(conn, request); err != nil {
				fail(t, "Test %d: request send: expected = <nil>, got = %v", i+1, err)
			}
		}

		received, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			fail(t, "Test %d: reply read: expected = <nil>, got = %v", i+1, err)
		}
		if received != reply {
			fail(t, "Test %d: reply: expected = %v, got = %v", i+1, reply, received)
		}

		conn.Close()
	}

	handleConnection := func(i int, wg *sync.WaitGroup, serverConn net.Conn, request, reply string) {
		defer wg.Done()

		received, err := bufio.NewReader(serverConn).ReadString('\n')
		if err != nil {
			fail(t, "Test %d: request read: expected = <nil>, got = %v", i+1, err)
		}

		if received != request {
			fail(t, "Test %d: request: expected = %v, got = %v", i+1, request, received)
		}

		if _, err := io.WriteString(serverConn, reply); err != nil {
			fail(t, "Test %d: reply send: expected = <nil>, got = %v", i+1, err)
		}

		serverConn.Close()
	}

	for i, testCase := range testCases {
		listener, err := newHTTPListener(
			testCase.serverAddrs,
			testCase.tlsConfig,
			time.Duration(0),
			time.Duration(0),
			time.Duration(0),
			nil,
			nil,
			nil,
		)
		if err != nil {
			t.Fatalf("Test %d: error: expected = <nil>, got = %v", i+1, err)
		}

		for _, serverAddr := range listener.Addrs() {
			go connect(i, serverAddr.String(), testCase.tlsConfig != nil, true, "GET /1 HTTP/1.0\n", testCase.reply)
			go connect(i, serverAddr.String(), testCase.tlsConfig != nil, false, "GET /2 HTTP/1.0\n", testCase.reply)

			var wg sync.WaitGroup

			serverConn, err := listener.Accept()
			if err != nil {
				t.Fatalf("Test %d: accept: expected = <nil>, got = %v", i+1, err)
			}
			wg.Add(1)
			go handleConnection(i, &wg, serverConn, "GET /2 HTTP/1.0\n", testCase.reply)

			serverConn, err = listener.Accept()
			if err != nil {
				t.Fatalf("Test %d: accept: expected = <nil>, got = %v", i+1, err)
			}
			wg.Add(1)
			go handleConnection(i, &wg, serverConn, "GET /1 HTTP/1.0\n", testCase.reply)

			wg.Wait()
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
			err:  &net.OpError{Err: &myTimeoutErr{timeout: false}},
			want: false,
		},
		{
			err:  io.ErrUnexpectedEOF,
			want: false,
		},
	}

	for i, tc := range testCases {
		if actual := ignoreErr(tc.err); actual != tc.want {
			t.Errorf("Test case %d: Expected %v but got %v for %v", i+1,
				tc.want, actual, tc.err)
		}
	}
}
