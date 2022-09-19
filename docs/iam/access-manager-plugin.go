//go:build ignore
// +build ignore

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
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
)

func writeErrorResponse(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusBadRequest)
	json.NewEncoder(w).Encode(map[string]string{
		"error": fmt.Sprintf("%v", err),
	})
}

type Result struct {
	Result bool `json:"result"`
}

func mainHandler(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeErrorResponse(w, err)
		return
	}

	reqMap := make(map[string]interface{})
	err = json.Unmarshal(body, &reqMap)
	if err != nil {
		writeErrorResponse(w, err)
		return
	}

	// fmt.Printf("request: %#v\n", reqMap)

	m := reqMap["input"].(map[string]interface{})
	accountValue := m["account"].(string)
	actionValue := m["action"].(string)

	// Allow user `minio` to perform any action.
	var res Result
	if accountValue == "minio" {
		res.Result = true
	} else {
		// All other users may not perform any `s3:Put*` operations.
		res.Result = true
		if strings.HasPrefix(actionValue, "s3:Put") {
			res.Result = false
		}
	}
	fmt.Printf("account: %v | action: %v | allowed: %v\n", accountValue, actionValue, res.Result)
	json.NewEncoder(w).Encode(res)
	return
}

func main() {
	http.HandleFunc("/", mainHandler)

	log.Print("Listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
