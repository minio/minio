package network

import (
	"encoding/json"
	"errors"
	"strings"
)

type MantleError struct {
	Message string `json:"message"`
	Body    string `json:"body"`
	Status  int    `json:"status"`
}

func parseMantleError(body []byte) error {
	var mantleError MantleError
	json.Unmarshal(body, &mantleError)
	return errors.New(strings.NewReplacer("[", "", "]", "", `"`, "").Replace(mantleError.Body))
}
