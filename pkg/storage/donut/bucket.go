package donut

import (
	"errors"
	"github.com/minio-io/iodine"
	"strconv"
	"strings"
)

type donutBucket struct {
	nodes   []string
	objects map[string][]byte
}

// GetNodes - get list of associated nodes for a given bucket
func (b donutBucket) GetNodes() ([]string, *iodine.Error) {
	var nodes []string
	for _, node := range b.nodes {
		nodes = append(nodes, node)
	}
	return nodes, nil
}

func (b donutBucket) AddNode(nodeID, bucketID string) *iodine.Error {
	tokens := strings.Split(bucketID, ":")
	if len(tokens) != 3 {
		return iodine.New(errors.New("Bucket ID malformed: "+bucketID), map[string]string{"nodeID": nodeID, "bucketID": bucketID})
	}
	// bucketName := tokens[0]
	// aggregate := tokens[1]
	// aggregate := "0"
	part, err := strconv.Atoi(tokens[2])
	if err != nil {
		return iodine.New(errors.New("Part malformed: "+tokens[2]), map[string]string{"nodeID": nodeID, "bucketID": bucketID})
	}
	b.nodes[part] = nodeID
	return nil
}
