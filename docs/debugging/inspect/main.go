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
	"bufio"
	crand "crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/klauspost/filepathx"
)

var (
	keyHex      = flag.String("key", "", "decryption key")
	privKeyPath = flag.String("private-key", "support_private.pem", "private key")
	stdin       = flag.Bool("stdin", false, "expect 'mc support inspect' json output from stdin")
	export      = flag.Bool("export", false, "export xl.meta")
	djson       = flag.Bool("djson", false, "expect djson format for xl.meta")
	genkey      = flag.Bool("genkey", false, "generate key pair")
)

func main() {
	flag.Parse()

	if *genkey {
		generateKeys()
		os.Exit(0)
	}
	var privateKey []byte
	if *keyHex == "" {
		if b, err := os.ReadFile(*privKeyPath); err == nil {
			privateKey = b
			fmt.Println("Using private key from", *privKeyPath)
		}

		// Prompt for decryption key if no --key or --private-key are provided
		if len(privateKey) == 0 && !*stdin {
			reader := bufio.NewReader(os.Stdin)
			fmt.Print("Enter Decryption Key: ")

			text, _ := reader.ReadString('\n')
			// convert CRLF to LF
			*keyHex = strings.ReplaceAll(text, "\n", "")
			*keyHex = strings.TrimSpace(*keyHex)
		}
	}

	var inputs []string

	// Parse parameters
	switch {
	case *stdin:
		// Parse 'mc support inspect --json' output
		input := struct {
			File string `json:"file"`
			Key  string `json:"key"`
		}{}
		got, err := io.ReadAll(os.Stdin)
		if err != nil {
			fatalErr(err)
		}
		fatalErr(json.Unmarshal(got, &input))
		inputs = []string{input.File}
		*keyHex = input.Key
	case len(flag.Args()) == 1:
		var err error
		inputs, err = filepathx.Glob(flag.Args()[0])
		fatalErr(err)
	default:
		flag.Usage()
		fatalIf(true, "Only 1 file can be decrypted")
		os.Exit(1)
	}
	for _, input := range inputs {
		processFile(input, privateKey)
	}
}

func processFile(inputFileName string, privateKey []byte) {
	// Calculate the output file name
	var outputFileName string
	switch {
	case strings.HasSuffix(inputFileName, ".enc"):
		outputFileName = strings.TrimSuffix(inputFileName, ".enc") + ".zip"
	case strings.HasSuffix(inputFileName, ".zip"):
		outputFileName = strings.TrimSuffix(inputFileName, ".zip") + ".decrypted.zip"
	case strings.Contains(inputFileName, ".enc."):
		outputFileName = strings.Replace(inputFileName, ".enc.", ".", 1) + ".zip"
	default:
		outputFileName = inputFileName + ".decrypted"
	}

	// Backup any already existing output file
	_, err := os.Stat(outputFileName)
	if err == nil {
		err := os.Rename(outputFileName, outputFileName+"."+time.Now().Format("20060102150405"))
		if err != nil {
			fatalErr(err)
		}
	}

	// Open the input and create the output file
	input, err := os.Open(inputFileName)
	fatalErr(err)
	defer input.Close()
	output, err := os.Create(outputFileName)
	fatalErr(err)

	// Decrypt the inspect data
	msg := fmt.Sprintf("output written to %s", outputFileName)

	switch {
	case *keyHex != "":
		err = extractInspectV1(*keyHex, input, output, msg)
	case len(privateKey) != 0:
		err = extractInspectV2(privateKey, input, output, msg)
	}
	output.Close()
	if err != nil {

		var keep keepFileErr
		if !errors.As(err, &keep) {
			os.Remove(outputFileName)
		}
		fatalErr(err)
	}

	// Export xl.meta to stdout
	if *export {
		fatalErr(inspectToExportType(outputFileName, *djson))
		os.Remove(outputFileName)
	}
}

func generateKeys() {
	privatekey, err := rsa.GenerateKey(crand.Reader, 2048)
	if err != nil {
		fmt.Printf("error generating key: %s n", err)
		os.Exit(1)
	}

	// dump private key to file
	privateKeyBytes := x509.MarshalPKCS1PrivateKey(privatekey)
	privateKeyBlock := &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: privateKeyBytes,
	}
	privatePem, err := os.Create("support_private.pem")
	if err != nil {
		fmt.Printf("error when create private.pem: %s n", err)
		os.Exit(1)
	}
	err = pem.Encode(privatePem, privateKeyBlock)
	if err != nil {
		fmt.Printf("error when encode private pem: %s n", err)
		os.Exit(1)
	}

	// dump public key to file
	publicKeyBytes := x509.MarshalPKCS1PublicKey(&privatekey.PublicKey)
	if err != nil {
		fmt.Printf("error when dumping publickey: %s n", err)
		os.Exit(1)
	}
	publicKeyBlock := &pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: publicKeyBytes,
	}
	publicPem, err := os.Create("support_public.pem")
	if err != nil {
		fmt.Printf("error when create public.pem: %s n", err)
		os.Exit(1)
	}
	err = pem.Encode(publicPem, publicKeyBlock)
	if err != nil {
		fmt.Printf("error when encode public pem: %s n", err)
		os.Exit(1)
	}
}
