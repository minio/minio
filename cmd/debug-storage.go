package cmd

import (
	"fmt"
	"io"
	"strings"
)

type debugStorage struct {
	path   string
	s      StorageAPI
	enable bool
}

func (d *debugStorage) String() string {
	return d.s.String()
}

func (d *debugStorage) IsOnline() bool {
	return d.s.IsOnline()
}

func (k *debugStorage) LastError() error {
	return nil
}

func (d *debugStorage) Close() error {
	return nil
}

func errStr(err error) string {
	if err == nil {
		return "<nil>"
	}
	return err.Error()
}

func (d *debugStorage) DiskInfo() (info DiskInfo, err error) {
	info, err = d.s.DiskInfo()
	if d.enable {
		fmt.Printf("%s: DiskInfo() (_, %s)\n", d.path, errStr(err))
	}
	return info, err
}

func (d *debugStorage) MakeVol(volume string) (err error) {
	err = d.s.MakeVol(volume)
	if d.enable {
		fmt.Printf("%s: MakeVol(%s) (%s)\n", d.path, volume, errStr(err))
	}
	return err
}

func (d *debugStorage) ListVols() (vols []VolInfo, err error) {
	vols, err = d.s.ListVols()
	var volNames []string
	for _, vol := range vols {
		volNames = append(volNames, vol.Name)
	}
	if d.enable {
		fmt.Printf("%s: ListVols() (%s, %s)\n", d.path, strings.Join(volNames, ":"), errStr(err))
	}
	return vols, err
}

func (d *debugStorage) StatVol(volume string) (vol VolInfo, err error) {
	vol, err = d.s.StatVol(volume)
	if d.enable {
		fmt.Printf("%s: StatVol(%s) (_, %s)\n", d.path, volume, errStr(err))
	}
	return vol, err
}

func (d *debugStorage) DeleteVol(volume string) (err error) {
	err = d.s.DeleteVol(volume)
	if d.enable {
		fmt.Printf("%s: DeleteVol(%s) (%s)\n", d.path, volume, errStr(err))
	}
	return err
}

func (d *debugStorage) ListDir(volume, dirPath string, count int) ([]string, error) {
	entries, err := d.s.ListDir(volume, dirPath, count)
	if d.enable {
		fmt.Printf("%s: ListDir(%s, %s, %d) (_, %s)\n", d.path, volume, dirPath, count, errStr(err))
	}
	return entries, err
}

func (d *debugStorage) ReadFile(volume string, path string, offset int64, buf []byte, verifier *BitrotVerifier) (n int64, err error) {
	n, err = d.s.ReadFile(volume, path, offset, buf, verifier)
	var algo string
	if verifier != nil {
		algo = verifier.algorithm.String()
	}
	if d.enable {
		fmt.Printf("%s: ReadFile(%s, %s, %d, len(buf)=%d, verifier={%s}) (_, %s)\n", d.path, volume, path, offset, len(buf), algo, errStr(err))
	}
	return n, err
}

func (d *debugStorage) AppendFile(volume string, path string, buf []byte) (err error) {
	err = d.s.AppendFile(volume, path, buf)
	if d.enable {
		fmt.Printf("%s: AppendFile(%s, %s, len(buf)=%d) (%s)\n", d.path, volume, path, len(buf), errStr(err))
	}
	return err
}

func (d *debugStorage) CreateFile(volume, filePath string, size int64, reader io.Reader) error {
	err := d.s.CreateFile(volume, filePath, size, reader)
	if d.enable {
		fmt.Printf("%s: CreateFile(%s, %s, %d) (%s)\n", d.path, volume, filePath, size, errStr(err))
	}
	return err
}

func (d *debugStorage) ReadFileStream(volume, filePath string, offset, length int64) (io.ReadCloser, error) {
	r, err := d.s.ReadFileStream(volume, filePath, offset, length)
	if d.enable {
		fmt.Printf("%s: ReadFileStream(%s, %s, %d, %d) (_, %s)\n", d.path, volume, filePath, offset, length, errStr(err))
	}
	return r, err
}

func (d *debugStorage) RenameFile(srcVolume, srcPath, dstVolume, dstPath string) error {
	err := d.s.RenameFile(srcVolume, srcPath, dstVolume, dstPath)
	if d.enable {
		fmt.Printf("%s: RenameFile(%s, %s, %s, %s) (%s)\n", d.path, srcVolume, srcPath, dstVolume, dstPath, errStr(err))
	}
	return err
}

func (d *debugStorage) StatFile(volume string, path string) (file FileInfo, err error) {
	fi, err := d.s.StatFile(volume, path)
	if d.enable {
		fmt.Printf("%s: StatFile(%s, %s) (_, %s)\n", d.path, volume, path, errStr(err))
	}
	return fi, err
}

func (d *debugStorage) DeleteFile(volume string, path string) (err error) {
	err = d.s.DeleteFile(volume, path)
	if d.enable {
		fmt.Printf("%s: DeleteFile(%s, %s) (_, %s)\n", d.path, volume, path, errStr(err))
	}
	return err
}

func (d *debugStorage) WriteAll(volume string, filePath string, buf []byte) (err error) {
	err = d.s.WriteAll(volume, filePath, buf)
	if d.enable {
		fmt.Printf("%s: WriteAll(%s, %s, len(buf)=%d) (%s)\n", d.path, volume, filePath, len(buf), errStr(err))
	}
	return err
}

func (d *debugStorage) ReadAll(volume string, filePath string) (buf []byte, err error) {
	buf, err = d.s.ReadAll(volume, filePath)
	if d.enable {
		fmt.Printf("%s: ReadAll(%s, %s) (len(buf)=%d, %s)\n", d.path, volume, filePath, len(buf), errStr(err))
	}
	return buf, err
}
