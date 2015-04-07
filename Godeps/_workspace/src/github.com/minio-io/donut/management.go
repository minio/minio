package donut

import (
	"encoding/json"
	"errors"
	"path"

	"github.com/minio-io/iodine"
)

func (d donut) Heal() error {
	return errors.New("Not Implemented")
}

func (d donut) Info() (nodeDiskMap map[string][]string, err error) {
	nodeDiskMap = make(map[string][]string)
	for nodeName, node := range d.nodes {
		disks, err := node.ListDisks()
		if err != nil {
			return nil, iodine.New(err, nil)
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
		return iodine.New(errors.New("invalid argument"), nil)
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
			return iodine.New(err, nil)
		}
		for _, disk := range disks {
			donutConfigPath := path.Join(d.name, donutConfig)
			donutConfigWriter, err := disk.MakeFile(donutConfigPath)
			defer donutConfigWriter.Close()
			if err != nil {
				return iodine.New(err, nil)
			}
			nodeDiskMap[hostname][disk.GetOrder()] = disk.GetPath()
			jenc := json.NewEncoder(donutConfigWriter)
			if err := jenc.Encode(nodeDiskMap); err != nil {
				return iodine.New(err, nil)
			}
		}
	}
	return nil
}

func (d donut) LoadConfig() error {
	return errors.New("Not Implemented")
}
