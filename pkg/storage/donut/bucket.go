package donut

type donutBucket struct {
	nodes   []string
	objects map[string][]byte
}

// GetNodes - get list of associated nodes for a given bucket
func (b donutBucket) GetNodes() ([]string, error) {
	var nodes []string
	for _, node := range b.nodes {
		nodes = append(nodes, node)
	}
	return nodes, nil
}
