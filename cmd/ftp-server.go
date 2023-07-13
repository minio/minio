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
	"context"
	"crypto/subtle"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/minio/cli"
	"github.com/minio/minio/internal/logger"
	"github.com/pkg/sftp"
	ftp "goftp.io/server/v2"
	"golang.org/x/crypto/ssh"
)

var globalRemoteFTPClientTransport = NewRemoteTargetHTTPTransport(true)()

// minioLogger use an instance of this to log in a standard format
type minioLogger struct{}

// Print implement Logger
func (log *minioLogger) Print(sessionID string, message interface{}) {
	if serverDebugLog {
		logger.Info("%s %s", sessionID, message)
	}
}

// Printf implement Logger
func (log *minioLogger) Printf(sessionID string, format string, v ...interface{}) {
	if serverDebugLog {
		if sessionID != "" {
			logger.Info("%s %s", sessionID, fmt.Sprintf(format, v...))
		} else {
			logger.Info(format, v...)
		}
	}
}

// PrintCommand impelment Logger
func (log *minioLogger) PrintCommand(sessionID string, command string, params string) {
	if serverDebugLog {
		if command == "PASS" {
			logger.Info("%s > PASS ****", sessionID)
		} else {
			logger.Info("%s > %s %s", sessionID, command, params)
		}
	}
}

// PrintResponse impelment Logger
func (log *minioLogger) PrintResponse(sessionID string, code int, message string) {
	if serverDebugLog {
		logger.Info("%s < %d %s", sessionID, code, message)
	}
}

func startSFTPServer(c *cli.Context) {
	args := c.StringSlice("sftp")

	var (
		port          int
		publicIP      string
		sshPrivateKey string
	)

	var err error
	for _, arg := range args {
		tokens := strings.SplitN(arg, "=", 2)
		if len(tokens) != 2 {
			logger.Fatal(fmt.Errorf("invalid arguments passed to --sftp=%s", arg), "unable to start SFTP server")
		}
		switch tokens[0] {
		case "address":
			host, portStr, err := net.SplitHostPort(tokens[1])
			if err != nil {
				logger.Fatal(fmt.Errorf("invalid arguments passed to --sftp=%s (%v)", arg, err), "unable to start SFTP server")
			}
			port, err = strconv.Atoi(portStr)
			if err != nil {
				logger.Fatal(fmt.Errorf("invalid arguments passed to --sftp=%s (%v)", arg, err), "unable to start SFTP server")
			}
			if port < 1 || port > 65535 {
				logger.Fatal(fmt.Errorf("invalid arguments passed to --sftp=%s, (port number must be between 1 to 65535)", arg), "unable to start SFTP server")
			}
			publicIP = host
		case "ssh-private-key":
			sshPrivateKey = tokens[1]
		}
	}

	if port == 0 {
		port = 8022 // Default SFTP port, since no port was given.
	}

	if sshPrivateKey == "" {
		logger.Fatal(fmt.Errorf("invalid arguments passed, private key file is mandatory for --sftp='ssh-private-key=path/to/id_ecdsa'"), "unable to start SFTP server")
	}

	privateBytes, err := os.ReadFile(sshPrivateKey)
	if err != nil {
		logger.Fatal(fmt.Errorf("invalid arguments passed, private key file is not accessible: %v", err), "unable to start SFTP server")
	}

	private, err := ssh.ParsePrivateKey(privateBytes)
	if err != nil {
		logger.Fatal(fmt.Errorf("invalid arguments passed, private key file is not parseable: %v", err), "unable to start SFTP server")
	}

	// An SSH server is represented by a ServerConfig, which holds
	// certificate details and handles authentication of ServerConns.
	config := &ssh.ServerConfig{
		PasswordCallback: func(c ssh.ConnMetadata, pass []byte) (*ssh.Permissions, error) {
			if globalIAMSys.LDAPConfig.Enabled() {
				targetUser, targetGroups, err := globalIAMSys.LDAPConfig.Bind(c.User(), string(pass))
				if err != nil {
					return nil, err
				}
				ldapPolicies, _ := globalIAMSys.PolicyDBGet(targetUser, false, targetGroups...)
				if len(ldapPolicies) == 0 {
					return nil, errAuthentication
				}
				return &ssh.Permissions{
					CriticalOptions: map[string]string{
						ldapUser:  targetUser,
						ldapUserN: c.User(),
					},
					Extensions: make(map[string]string),
				}, nil
			}

			ui, ok := globalIAMSys.GetUser(context.Background(), c.User())
			if !ok {
				return nil, errNoSuchUser
			}

			if subtle.ConstantTimeCompare([]byte(ui.Credentials.SecretKey), pass) == 1 {
				return &ssh.Permissions{
					CriticalOptions: map[string]string{
						"accessKey": c.User(),
					},
					Extensions: make(map[string]string),
				}, nil
			}
			return nil, errAuthentication
		},
	}

	config.AddHostKey(private)

	// Once a ServerConfig has been configured, connections can be accepted.
	listener, err := net.Listen("tcp", net.JoinHostPort(publicIP, strconv.Itoa(port)))
	if err != nil {
		logger.Fatal(err, "unable to start listening on --sftp='port=%d'", port)
	}

	logger.Info(fmt.Sprintf("MinIO SFTP Server listening on %s", net.JoinHostPort(publicIP, strconv.Itoa(port))))

	for {
		nConn, err := listener.Accept()
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}

		// Before use, a handshake must be performed on the incoming net.Conn.
		sconn, chans, reqs, err := ssh.NewServerConn(nConn, config)
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}

		// The incoming Request channel must be serviced.
		go ssh.DiscardRequests(reqs)

		// Service the incoming Channel channel.
		for newChannel := range chans {
			// Channels have a type, depending on the application level
			// protocol intended. In the case of an SFTP session, this is "subsystem"
			// with a payload string of "<length=4>sftp"
			if newChannel.ChannelType() != "session" {
				newChannel.Reject(ssh.UnknownChannelType, "unknown channel type")
				continue
			}
			channel, requests, err := newChannel.Accept()
			if err != nil {
				logger.Fatal(err, "unable to accept the connection requests channel")
			}

			// Sessions have out-of-band requests such as "shell",
			// "pty-req" and "env".  Here we handle only the
			// "subsystem" request.
			go func(in <-chan *ssh.Request) {
				for req := range in {
					// We only reply to SSH packets that have `sftp` payload.
					req.Reply(req.Type == "subsystem" && string(req.Payload[4:]) == "sftp", nil)
				}
			}(requests)

			server := sftp.NewRequestServer(channel, NewSFTPDriver(sconn.Permissions))
			if err := server.Serve(); err == io.EOF {
				server.Close()
			} else if err != nil {
				logger.Fatal(err, "unable to start SFTP server")
			}
		}
	}
}

func startFTPServer(c *cli.Context) {
	args := c.StringSlice("ftp")

	var (
		port          int
		publicIP      string
		portRange     string
		tlsPrivateKey string
		tlsPublicCert string
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

	name := "MinIO FTP Server"
	if tls {
		name = "MinIO FTP(Secure) Server"
	}

	ftpServer, err := ftp.NewServer(&ftp.Options{
		Name:           name,
		WelcomeMessage: fmt.Sprintf("Welcome to MinIO FTP Server Version='%s' License='GNU AGPLv3'", Version),
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
	})
	if err != nil {
		logger.Fatal(err, "unable to initialize FTP server")
	}

	logger.Info(fmt.Sprintf("%s listening on %s", name, net.JoinHostPort(publicIP, strconv.Itoa(port))))

	if err = ftpServer.ListenAndServe(); err != nil {
		logger.Fatal(err, "unable to start FTP server")
	}
}
