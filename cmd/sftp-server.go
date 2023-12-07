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
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio/internal/logger"
	xsftp "github.com/minio/pkg/v2/sftp"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

type sftpLogger struct{}

func (s *sftpLogger) Info(tag xsftp.LogType, msg string) {
	logger.Info(msg)
}

func (s *sftpLogger) Error(tag xsftp.LogType, err error) {
	switch tag {
	case xsftp.AcceptNetworkError:
		logger.LogOnceIf(context.Background(), err, "accept-limit-sftp")
	case xsftp.AcceptChannelError:
		logger.LogOnceIf(context.Background(), err, "accept-channel-sftp")
	case xsftp.SSHKeyExchangeError:
		logger.LogOnceIf(context.Background(), err, "key-exchange-sftp")
	default:
		logger.LogOnceIf(context.Background(), err, "unknown-error-sftp")
	}
}

func startSFTPServer(args []string) {
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
	sshConfig := &ssh.ServerConfig{
		PasswordCallback: func(c ssh.ConnMetadata, pass []byte) (*ssh.Permissions, error) {
			if globalIAMSys.LDAPConfig.Enabled() {
				sa, _, err := globalIAMSys.getServiceAccount(context.Background(), c.User())
				if err != nil && !errors.Is(err, errNoSuchServiceAccount) {
					return nil, err
				}
				if errors.Is(err, errNoSuchServiceAccount) {
					targetUser, targetGroups, err := globalIAMSys.LDAPConfig.Bind(c.User(), string(pass))
					if err != nil {
						return nil, err
					}
					ldapPolicies, _ := globalIAMSys.PolicyDBGet(targetUser, targetGroups...)
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
				if subtle.ConstantTimeCompare([]byte(sa.Credentials.SecretKey), pass) == 1 {
					return &ssh.Permissions{
						CriticalOptions: map[string]string{
							"accessKey": c.User(),
						},
						Extensions: make(map[string]string),
					}, nil
				}
				return nil, errAuthentication
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

	sshConfig.AddHostKey(private)

	handleSFTPSession := func(channel ssh.Channel, sconn *ssh.ServerConn) {
		server := sftp.NewRequestServer(channel, NewSFTPDriver(sconn.Permissions), sftp.WithRSAllocator())
		defer server.Close()
		server.Serve()
	}

	sftpServer, err := xsftp.NewServer(&xsftp.Options{
		PublicIP: publicIP,
		Port:     port,
		// OpensSSH default handshake timeout is 2 minutes.
		SSHHandshakeDeadline: 2 * time.Minute,
		Logger:               new(sftpLogger),
		SSHConfig:            sshConfig,
		HandleSFTPSession:    handleSFTPSession,
	})
	if err != nil {
		logger.Fatal(err, "Unable to start SFTP Server")
	}

	err = sftpServer.Listen()
	if err != nil {
		logger.Fatal(err, "SFTP Server had an unrecoverable error while accepting connections")
	}
}
