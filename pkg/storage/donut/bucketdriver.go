package donut

type bucketDriver struct {
	nodes   []string
	objects map[string][]byte
}

func (b bucketDriver) GetNodes() ([]string, error) {
	var nodes []string
	for _, node := range b.nodes {
		nodes = append(nodes, node)
	}
	return nodes, nil
}
