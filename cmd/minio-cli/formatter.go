// this code is from gofmt, modified for our internal usage - http://golang.org/src/cmd/gofmt/gofmt.go
package main

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

func processFile(filename string, in io.Reader, out io.Writer, stdin bool) error {
	if in == nil {
		f, err := os.Open(filename)
		if err != nil {
			return err
		}
		defer f.Close()
		in = f
	}

	src, err := ioutil.ReadAll(in)
	if err != nil {
		return err
	}
	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, filename, src, parser.ParseComments)
	if err != nil {
		return err
	}

	ast.SortImports(fileSet, file)

	var buf bytes.Buffer
	tabWidth := 8
	printerMode := printer.UseSpaces | printer.TabIndent
	err = (&printer.Config{Mode: printerMode, Tabwidth: tabWidth}).Fprint(&buf, fileSet, file)
	if err != nil {
		return err
	}

	res := buf.Bytes()
	if !bytes.Equal(src, res) {
		err = ioutil.WriteFile(filename, res, 0)
		if err != nil {
			return err
		}
	}
	return nil
}

func isGofile(f os.FileInfo) bool {
	name := f.Name()
	return !f.IsDir() && !strings.HasPrefix(name, ".") && strings.HasSuffix(name, ".go")
}

func visitFile(pathName string, f os.FileInfo, err error) error {

	if err == nil && isGofile(f) {
		err = processFile(pathName, nil, os.Stdout, false)
	}

	if err != nil {
		return err
	}

	return nil
}

func walkDir(pathName string) {
	filepath.Walk(pathName, visitFile)
}

func GoFormat(pathName string) error {
	dir, err := os.Stat(pathName)
	if err != nil {
		return err
	}

	if dir.IsDir() {
		walkDir(pathName)
	}
	return nil
}
