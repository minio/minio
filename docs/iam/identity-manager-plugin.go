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
	"errors"
	"fmt"
	"log"
	"net/http"
)

func writeErrorResponse(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusBadRequest)
	json.NewEncoder(w).Encode(map[string]string{
		"reason": fmt.Sprintf("%v", err),
	})
}

type Resp struct {
	User               string                 `json:"user"`
	MaxValiditySeconds int                    `json:"maxValiditySeconds"`
	Claims             map[string]interface{} `json:"claims"`
}

var tokens map[string]Resp = map[string]Resp{
	"aaa": {
		User:               "Alice",
		MaxValiditySeconds: 3600,
		Claims: map[string]interface{}{
			"groups": []string{"data-science"},
		},
	},
	"bbb": {
		User:               "Bart",
		MaxValiditySeconds: 3600,
		Claims: map[string]interface{}{
			"groups": []string{"databases"},
		},
	},
}

func mainHandler(w http.ResponseWriter, r *http.Request) {
	token := r.FormValue("token")
	if token == "" {
		writeErrorResponse(w, errors.New("token parameter not given"))
		return
	}

	rsp, ok := tokens[token]
	if !ok {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	fmt.Printf("Allowed for token: %s user: %s\n", token, rsp.User)

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(rsp)
	return
}

func main() {
	http.HandleFunc("/", mainHandler)

	log.Print("Listening on :8081")
	log.Fatal(http.ListenAndServe(":8081", nil))
}
