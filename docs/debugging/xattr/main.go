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

package main

import (
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/olekukonko/tablewriter"
	"github.com/pkg/xattr"
)

var (
	path, name string
	value      uint64
	set, list  bool
)

func getxattr(path, name string) (uint64, error) {
	buf, err := xattr.LGet(path, name)
	if err != nil {
		return 0, err
	}

	return binary.LittleEndian.Uint64(buf[:8]), nil
}

func listxattr(path string) ([]string, error) {
	return xattr.LList(path)
}

func setxattr(path, name string, value uint64) error {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, value)
	return xattr.LSet(path, name, data)
}

func main() {
	flag.StringVar(&path, "path", "", "path name where the attribute shall be applied")
	flag.StringVar(&name, "name", "", "attribute name or it can be a wildcard if '.' is specified")
	flag.Uint64Var(&value, "value", 0, "attribute value expects the value to be uint64")
	flag.BoolVar(&set, "set", false, "this is a set attribute operation")

	flag.Parse()

	if set && value == 0 {
		log.Fatalln("setting an attribute requires a non-zero value")
	}

	if !set && value > 0 {
		log.Fatalln("to set a value please specify --set along with --value")
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Name", "Value"})
	table.SetAutoWrapText(false)
	table.SetAutoFormatHeaders(true)
	table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.SetCenterSeparator("")
	table.SetColumnSeparator("")
	table.SetRowSeparator("")
	table.SetHeaderLine(false)
	// table.EnableBorder(false)
	table.SetTablePadding("\t") // pad with tabs
	table.SetNoWhiteSpace(true)

	if set {
		if err := setxattr(path, name, value); err != nil {
			log.Fatalln(fmt.Errorf("setting attribute %s failed with: %v", name, err))
		}
	} else {
		if name == "" {
			log.Fatalln("you must specify an attribute name for reading")
		}
		var names []string
		if name == "." {
			attrs, err := listxattr(path)
			if err != nil {
				log.Fatalln(fmt.Errorf("listing attributes failed with: %v", err))
			}
			names = append(names, attrs...)
		} else {
			names = append(names, name)
		}
		var data [][]string
		for _, attr := range names {
			value, err := getxattr(path, attr)
			if err != nil {
				data = append(data, []string{attr, errors.Unwrap(err).Error()})
			} else {
				data = append(data, []string{attr, fmt.Sprintf("%d", value)})
			}
		}
		table.AppendBulk(data) // Add Bulk Data
		table.Render()
	}
}
