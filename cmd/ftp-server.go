// Copyright (c) 2015-2023 MinIO, Inc.
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

package cmd

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/minio/minio/internal/logger"
	ftp "goftp.io/server/v2"
)

var globalRemoteFTPClientTransport = NewRemoteTargetHTTPTransport(true)()

// minioLogger use an instance of this to log in a standard format
type minioLogger struct{}

// Print implement Logger
func (log *minioLogger) Print(sessionID string, message any) {
	if serverDebugLog {
		fmt.Printf("%s %s\n", sessionID, message)
	}
}

// Printf implement Logger
func (log *minioLogger) Printf(sessionID string, format string, v ...any) {
	if serverDebugLog {
		if sessionID != "" {
			fmt.Printf("%s %s\n", sessionID, fmt.Sprintf(format, v...))
		} else {
			fmt.Printf(format+"\n", v...)
		}
	}
}

// PrintCommand implement Logger
func (log *minioLogger) PrintCommand(sessionID string, command string, params string) {
	if serverDebugLog {
		if command == "PASS" {
			fmt.Printf("%s > PASS ****\n", sessionID)
		} else {
			fmt.Printf("%s > %s %s\n", sessionID, command, params)
		}
	}
}

// PrintResponse implement Logger
func (log *minioLogger) PrintResponse(sessionID string, code int, message string) {
	if serverDebugLog {
		fmt.Printf("%s < %d %s\n", sessionID, code, message)
	}
}

func startFTPServer(args []string) {
	var (
		port          int
		publicIP      string
		portRange     string
		tlsPrivateKey string
		tlsPublicCert string
		forceTLS      bool
	)

	var err error
	for _, arg := range args {
		tokens := strings.SplitN(arg, "=", 2)
		if len(tokens) != 2 {
			logger.Fatal(fmt.Errorf("invalid arguments passed to --ftp=%s", arg), "unable to start FTP server")
		}
		switch tokens[0] {
		case "address":
			host, portStr, err := net.SplitHostPort(tokens[1])
			if err != nil {
				logger.Fatal(fmt.Errorf("invalid arguments passed to --ftp=%s (%v)", arg, err), "unable to start FTP server")
			}
			port, err = strconv.Atoi(portStr)
			if err != nil {
				logger.Fatal(fmt.Errorf("invalid arguments passed to --ftp=%s (%v)", arg, err), "unable to start FTP server")
			}
			if port < 1 || port > 65535 {
				logger.Fatal(fmt.Errorf("invalid arguments passed to --ftp=%s, (port number must be between 1 to 65535)", arg), "unable to start FTP server")
			}
			publicIP = host
		case "passive-port-range":
			portRange = tokens[1]
		case "tls-private-key":
			tlsPrivateKey = tokens[1]
		case "tls-public-cert":
			tlsPublicCert = tokens[1]
		case "force-tls":
			forceTLS, err = strconv.ParseBool(tokens[1])
			if err != nil {
				logger.Fatal(fmt.Errorf("invalid arguments passed to --ftp=%s (%v)", arg, err), "unable to start FTP server")
			}
		}
	}

	// Verify if only partial inputs are given for FTP(secure)
	{
		if tlsPrivateKey == "" && tlsPublicCert != "" {
			logger.Fatal(fmt.Errorf("invalid TLS arguments provided missing private key --ftp=\"tls-private-key=path/to/private.key\""), "unable to start FTP server")
		}

		if tlsPrivateKey != "" && tlsPublicCert == "" {
			logger.Fatal(fmt.Errorf("invalid TLS arguments provided missing public cert --ftp=\"tls-public-cert=path/to/public.crt\""), "unable to start FTP server")
		}
		if port == 0 {
			port = 8021 // Default FTP port, since no port was given.
		}
	}

	// If no TLS certs were provided, server is running in TLS for S3 API
	// we automatically make FTP also run under TLS mode.
	if globalIsTLS && tlsPrivateKey == "" && tlsPublicCert == "" {
		tlsPrivateKey = getPrivateKeyFile()
		tlsPublicCert = getPublicCertFile()
	}

	tls := tlsPrivateKey != "" && tlsPublicCert != ""

	if forceTLS && !tls {
		logger.Fatal(fmt.Errorf("invalid TLS arguments provided. force-tls, but missing private key --ftp=\"tls-private-key=path/to/private.key\""), "unable to start FTP server")
	}

	name := "MinIO FTP Server"
	if tls {
		name = "MinIO FTP(Secure) Server"
	}

	ftpServer, err := ftp.NewServer(&ftp.Options{
		Name:           name,
		WelcomeMessage: fmt.Sprintf("Welcome to '%s' FTP Server Version='%s' License='%s'", MinioStoreName, MinioLicense, Version),
		Driver:         NewFTPDriver(),
		Port:           port,
		Perm:           ftp.NewSimplePerm("nobody", "nobody"),
		TLS:            tls,
		KeyFile:        tlsPrivateKey,
		CertFile:       tlsPublicCert,
		ExplicitFTPS:   tls,
		Logger:         &minioLogger{},
		PassivePorts:   portRange,
		PublicIP:       publicIP,
		ForceTLS:       forceTLS,
	})
	if err != nil {
		logger.Fatal(err, "unable to initialize FTP server")
	}

	logger.Info(fmt.Sprintf("%s listening on %s", name, net.JoinHostPort(publicIP, strconv.Itoa(port))))

	if err = ftpServer.ListenAndServe(); err != nil {
		logger.Fatal(err, "unable to start FTP server")
	}
}
