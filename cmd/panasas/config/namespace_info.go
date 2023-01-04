package config

import (
	"encoding/json"
	"time"
)

// NamespaceInfo represents information about Panasas config agent namespace
type NamespaceInfo struct {
	ID        string    `json:"id"`
	Revision  string    `json:"revision"`
	Timestamp time.Time `json:"timestamp"`
}

func parseNamespaceInfo(JSONData string) (*NamespaceInfo, error) {
	var ni NamespaceInfo

	if err := json.Unmarshal([]byte(JSONData), &ni); err != nil {
		return nil, err
	}
	return &ni, nil
}
