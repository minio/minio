package donut

import (
	"encoding/json"
	"errors"
	"path"
)

func (d donut) Heal() error {
	return errors.New("Not Implemented")
}

func (d donut) Info() (nodeDiskMap map[string][]string, err error) {
	nodeDiskMap = make(map[string][]string)
	for nodeName, node := range d.nodes {
		disks, err := node.ListDisks()
		if err != nil {
			return nil, err
		}
		diskList := make([]string, len(disks))
		for diskName, disk := range disks {
			diskList[disk.GetOrder()] = diskName
		}
		nodeDiskMap[nodeName] = diskList
	}
	return nodeDiskMap, nil
}

func (d donut) AttachNode(node Node) error {
	if node == nil {
		return errors.New("invalid argument")
	}
	d.nodes[node.GetNodeName()] = node
	return nil
}
func (d donut) DetachNode(node Node) error {
	delete(d.nodes, node.GetNodeName())
	return nil
}

func (d donut) SaveConfig() error {
	nodeDiskMap := make(map[string][]string)
	for hostname, node := range d.nodes {
		disks, err := node.ListDisks()
		if err != nil {
			return err
		}
		for _, disk := range disks {
			donutConfigPath := path.Join(d.name, donutConfig)
			donutConfigWriter, err := disk.MakeFile(donutConfigPath)
			defer donutConfigWriter.Close()
			if err != nil {
				return err
			}
			nodeDiskMap[hostname][disk.GetOrder()] = disk.GetPath()
			jenc := json.NewEncoder(donutConfigWriter)
			if err := jenc.Encode(nodeDiskMap); err != nil {
				return err
			}
		}
	}
	return nil
}

func (d donut) LoadConfig() error {
	return errors.New("Not Implemented")
}
