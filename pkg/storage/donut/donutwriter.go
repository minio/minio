package donut

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path"
)

func newDonutFileWriter(objectDir string) (Writer, error) {
	dataFile, err := os.OpenFile(path.Join(objectDir, "data"), os.O_WRONLY|os.O_CREATE|os.O_TRUNC|os.O_EXCL, 0600)
	if err != nil {
		return nil, err
	}
	return donutFileWriter{
		root:          objectDir,
		file:          dataFile,
		metadata:      make(map[string]string),
		donutMetadata: make(map[string]string),
	}, nil
}

type donutFileWriter struct {
	root          string
	file          *os.File
	metadata      map[string]string
	donutMetadata map[string]string
	err           error
}

func (d donutFileWriter) Write(data []byte) (int, error) {
	return d.file.Write(data)
}

func (d donutFileWriter) Close() error {
	if d.err != nil {
		return d.err
	}
	metadata, _ := json.Marshal(d.metadata)
	ioutil.WriteFile(path.Join(d.root, "metadata.json"), metadata, 0600)
	donutMetadata, _ := json.Marshal(d.donutMetadata)
	ioutil.WriteFile(path.Join(d.root, "donutMetadata.json"), donutMetadata, 0600)

	return d.file.Close()
}

func (d donutFileWriter) CloseWithError(err error) error {
	if d.err != nil {
		d.err = err
	}
	return d.Close()
}

func (d donutFileWriter) SetMetadata(metadata map[string]string) error {
	for k := range d.metadata {
		delete(d.metadata, k)
	}
	for k, v := range metadata {
		d.metadata[k] = v
	}
	return nil
}

func (d donutFileWriter) GetMetadata() (map[string]string, error) {
	metadata := make(map[string]string)
	for k, v := range d.metadata {
		metadata[k] = v
	}
	return metadata, nil
}

func (d donutFileWriter) SetDonutMetadata(metadata map[string]string) error {
	for k := range d.donutMetadata {
		delete(d.donutMetadata, k)
	}
	for k, v := range metadata {
		d.donutMetadata[k] = v
	}
	return nil
}

func (d donutFileWriter) GetDonutMetadata() (map[string]string, error) {
	donutMetadata := make(map[string]string)
	for k, v := range d.donutMetadata {
		donutMetadata[k] = v
	}
	return donutMetadata, nil
}
