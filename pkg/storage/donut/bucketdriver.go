package donut

type bucketDriver struct {
	nodes   []string
	objects map[string][]byte
}

func (self bucketDriver) GetNodes() ([]string, error) {
	var nodes []string
	for _, node := range self.nodes {
		nodes = append(nodes, node)
	}
	return nodes, nil
}
