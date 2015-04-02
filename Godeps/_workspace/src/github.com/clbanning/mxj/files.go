package mxj

import (
	"fmt"
	"io"
	"os"
)

type Maps []Map

func NewMaps() Maps {
	return make(Maps, 0)
}

type MapRaw struct {
	M Map
	R []byte
}

// NewMapsFromXmlFile - creates an array from a file of JSON values.
func NewMapsFromJsonFile(name string) (Maps, error) {
	fi, err := os.Stat(name)
	if err != nil {
		return nil, err
	}
	if !fi.Mode().IsRegular() {
		return nil, fmt.Errorf("file %s is not a regular file", name)
	}

	fh, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer fh.Close()

	am := make([]Map, 0)
	for {
		m, raw, err := NewMapJsonReaderRaw(fh)
		if err != nil && err != io.EOF {
			return am, fmt.Errorf("error: %s - reading: %s", err.Error(), string(raw))
		}
		if len(m) > 0 {
			am = append(am, m)
		}
		if err == io.EOF {
			break
		}
	}
	return am, nil
}

// ReadMapsFromJsonFileRaw - creates an array of MapRaw from a file of JSON values.
func NewMapsFromJsonFileRaw(name string) ([]MapRaw, error) {
	fi, err := os.Stat(name)
	if err != nil {
		return nil, err
	}
	if !fi.Mode().IsRegular() {
		return nil, fmt.Errorf("file %s is not a regular file", name)
	}

	fh, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer fh.Close()

	am := make([]MapRaw, 0)
	for {
		mr := new(MapRaw)
		mr.M, mr.R, err = NewMapJsonReaderRaw(fh)
		if err != nil && err != io.EOF {
			return am, fmt.Errorf("error: %s - reading: %s", err.Error(), string(mr.R))
		}
		if len(mr.M) > 0 {
			am = append(am, *mr)
		}
		if err == io.EOF {
			break
		}
	}
	return am, nil
}

// NewMapsFromXmlFile - creates an array from a file of XML values.
func NewMapsFromXmlFile(name string) (Maps, error) {
	x := XmlWriterBufSize
	XmlWriterBufSize = 0
	defer func() {
		XmlWriterBufSize = x
	}()

	fi, err := os.Stat(name)
	if err != nil {
		return nil, err
	}
	if !fi.Mode().IsRegular() {
		return nil, fmt.Errorf("file %s is not a regular file", name)
	}

	fh, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer fh.Close()

	am := make([]Map, 0)
	for {
		m, raw, err := NewMapXmlReaderRaw(fh)
		if err != nil && err != io.EOF {
			return am, fmt.Errorf("error: %s - reading: %s", err.Error(), string(raw))
		}
		if len(m) > 0 {
			am = append(am, m)
		}
		if err == io.EOF {
			break
		}
	}
	return am, nil
}

// NewMapsFromXmlFileRaw - creates an array of MapRaw from a file of XML values.
// NOTE: the slice with the raw XML is clean with no extra capacity - unlike NewMapXmlReaderRaw().
// It is slow at parsing a file from disk and is intended for relatively small utility files.
func NewMapsFromXmlFileRaw(name string) ([]MapRaw, error) {
	x := XmlWriterBufSize
	XmlWriterBufSize = 0
	defer func() {
		XmlWriterBufSize = x
	}()

	fi, err := os.Stat(name)
	if err != nil {
		return nil, err
	}
	if !fi.Mode().IsRegular() {
		return nil, fmt.Errorf("file %s is not a regular file", name)
	}

	fh, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer fh.Close()

	am := make([]MapRaw, 0)
	for {
		mr := new(MapRaw)
		mr.M, mr.R, err = NewMapXmlReaderRaw(fh)
		if err != nil && err != io.EOF {
			return am, fmt.Errorf("error: %s - reading: %s", err.Error(), string(mr.R))
		}
		if len(mr.M) > 0 {
			am = append(am, *mr)
		}
		if err == io.EOF {
			break
		}
	}
	return am, nil
}

// ------------------------ Maps writing -------------------------
// These are handy-dandy methods for dumping configuration data, etc.

// JsonString - analogous to mv.Json()
func (mvs Maps) JsonString(safeEncoding ...bool) (string, error) {
	var s string
	for _, v := range mvs {
		j, err := v.Json()
		if err != nil {
			return s, err
		}
		s += string(j)
	}
	return s, nil
}

// JsonStringIndent - analogous to mv.JsonIndent()
func (mvs Maps) JsonStringIndent(prefix, indent string, safeEncoding ...bool) (string, error) {
	var s string
	var haveFirst bool
	for _, v := range mvs {
		j, err := v.JsonIndent(prefix, indent)
		if err != nil {
			return s, err
		}
		if haveFirst {
			s += "\n"
		} else {
			haveFirst = true
		}
		s += string(j)
	}
	return s, nil
}

// XmlString - analogous to mv.Xml()
func (mvs Maps) XmlString() (string, error) {
	var s string
	for _, v := range mvs {
		x, err := v.Xml()
		if err != nil {
			return s, err
		}
		s += string(x)
	}
	return s, nil
}

// XmlStringIndent - analogous to mv.XmlIndent()
func (mvs Maps) XmlStringIndent(prefix, indent string) (string, error) {
	var s string
	for _, v := range mvs {
		x, err := v.XmlIndent(prefix, indent)
		if err != nil {
			return s, err
		}
		s += string(x)
	}
	return s, nil
}

// JsonFile - write Maps to named file as JSON
// Note: the file will be created, if necessary; if it exists it will be truncated.
// If you need to append to a file, open it and use JsonWriter method.
func (mvs Maps) JsonFile(file string, safeEncoding ...bool) error {
	var encoding bool
	if len(safeEncoding) == 1 {
		encoding = safeEncoding[0]
	}
	s, err := mvs.JsonString(encoding)
	if err != nil {
		return err
	}
	fh, err := os.Create(file)
	if err != nil {
		return err
	}
	defer fh.Close()
	fh.WriteString(s)
	return nil
}

// JsonFileIndent - write Maps to named file as pretty JSON
// Note: the file will be created, if necessary; if it exists it will be truncated.
// If you need to append to a file, open it and use JsonIndentWriter method.
func (mvs Maps) JsonFileIndent(file, prefix, indent string, safeEncoding ...bool) error {
	var encoding bool
	if len(safeEncoding) == 1 {
		encoding = safeEncoding[0]
	}
	s, err := mvs.JsonStringIndent(prefix, indent, encoding)
	if err != nil {
		return err
	}
	fh, err := os.Create(file)
	if err != nil {
		return err
	}
	defer fh.Close()
	fh.WriteString(s)
	return nil
}

// XmlFile - write Maps to named file as XML
// Note: the file will be created, if necessary; if it exists it will be truncated.
// If you need to append to a file, open it and use XmlWriter method.
func (mvs Maps) XmlFile(file string) error {
	s, err := mvs.XmlString()
	if err != nil {
		return err
	}
	fh, err := os.Create(file)
	if err != nil {
		return err
	}
	defer fh.Close()
	fh.WriteString(s)
	return nil
}

// XmlFileIndent - write Maps to named file as pretty XML
// Note: the file will be created,if necessary; if it exists it will be truncated.
// If you need to append to a file, open it and use XmlIndentWriter method.
func (mvs Maps) XmlFileIndent(file, prefix, indent string) error {
	s, err := mvs.XmlStringIndent(prefix, indent)
	if err != nil {
		return err
	}
	fh, err := os.Create(file)
	if err != nil {
		return err
	}
	defer fh.Close()
	fh.WriteString(s)
	return nil
}
