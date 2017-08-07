package binarydist

import (
	"io"
	"os/exec"
)

type bzip2Writer struct {
	c *exec.Cmd
	w io.WriteCloser
}

func (w bzip2Writer) Write(b []byte) (int, error) {
	return w.w.Write(b)
}

func (w bzip2Writer) Close() error {
	if err := w.w.Close(); err != nil {
		return err
	}
	return w.c.Wait()
}

// Package compress/bzip2 implements only decompression,
// so we'll fake it by running bzip2 in another process.
func newBzip2Writer(w io.Writer) (wc io.WriteCloser, err error) {
	var bw bzip2Writer
	bw.c = exec.Command("bzip2", "-c")
	bw.c.Stdout = w

	if bw.w, err = bw.c.StdinPipe(); err != nil {
		return nil, err
	}

	if err = bw.c.Start(); err != nil {
		return nil, err
	}

	return bw, nil
}
