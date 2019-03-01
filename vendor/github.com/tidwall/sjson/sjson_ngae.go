//+build !appengine

package sjson

import (
	jsongo "encoding/json"
	"reflect"
	"strconv"
	"unsafe"

	"github.com/tidwall/gjson"
)

func set(jstr, path, raw string,
	stringify, del, optimistic, inplace bool) ([]byte, error) {
	if path == "" {
		return nil, &errorType{"path cannot be empty"}
	}
	if !del && optimistic && isOptimisticPath(path) {
		res := gjson.Get(jstr, path)
		if res.Exists() && res.Index > 0 {
			sz := len(jstr) - len(res.Raw) + len(raw)
			if stringify {
				sz += 2
			}
			if inplace && sz <= len(jstr) {
				if !stringify || !mustMarshalString(raw) {
					jsonh := *(*reflect.StringHeader)(unsafe.Pointer(&jstr))
					jsonbh := reflect.SliceHeader{
						Data: jsonh.Data, Len: jsonh.Len, Cap: jsonh.Len}
					jbytes := *(*[]byte)(unsafe.Pointer(&jsonbh))
					if stringify {
						jbytes[res.Index] = '"'
						copy(jbytes[res.Index+1:], []byte(raw))
						jbytes[res.Index+1+len(raw)] = '"'
						copy(jbytes[res.Index+1+len(raw)+1:],
							jbytes[res.Index+len(res.Raw):])
					} else {
						copy(jbytes[res.Index:], []byte(raw))
						copy(jbytes[res.Index+len(raw):],
							jbytes[res.Index+len(res.Raw):])
					}
					return jbytes[:sz], nil
				}
				return nil, nil
			}
			buf := make([]byte, 0, sz)
			buf = append(buf, jstr[:res.Index]...)
			if stringify {
				buf = appendStringify(buf, raw)
			} else {
				buf = append(buf, raw...)
			}
			buf = append(buf, jstr[res.Index+len(res.Raw):]...)
			return buf, nil
		}
	}
	// parse the path, make sure that it does not contain invalid characters
	// such as '#', '?', '*'
	paths := make([]pathResult, 0, 4)
	r, err := parsePath(path)
	if err != nil {
		return nil, err
	}
	paths = append(paths, r)
	for r.more {
		if r, err = parsePath(r.path); err != nil {
			return nil, err
		}
		paths = append(paths, r)
	}

	njson, err := appendRawPaths(nil, jstr, paths, raw, stringify, del)
	if err != nil {
		return nil, err
	}
	return njson, nil
}

// SetOptions sets a json value for the specified path with options.
// A path is in dot syntax, such as "name.last" or "age".
// This function expects that the json is well-formed, and does not validate.
// Invalid json will not panic, but it may return back unexpected results.
// An error is returned if the path is not valid.
func SetOptions(json, path string, value interface{},
	opts *Options) (string, error) {
	if opts != nil {
		if opts.ReplaceInPlace {
			// it's not safe to replace bytes in-place for strings
			// copy the Options and set options.ReplaceInPlace to false.
			nopts := *opts
			opts = &nopts
			opts.ReplaceInPlace = false
		}
	}
	jsonh := *(*reflect.StringHeader)(unsafe.Pointer(&json))
	jsonbh := reflect.SliceHeader{Data: jsonh.Data, Len: jsonh.Len}
	jsonb := *(*[]byte)(unsafe.Pointer(&jsonbh))
	res, err := SetBytesOptions(jsonb, path, value, opts)
	return string(res), err
}

// SetBytesOptions sets a json value for the specified path with options.
// If working with bytes, this method preferred over
// SetOptions(string(data), path, value)
func SetBytesOptions(json []byte, path string, value interface{},
	opts *Options) ([]byte, error) {
	var optimistic, inplace bool
	if opts != nil {
		optimistic = opts.Optimistic
		inplace = opts.ReplaceInPlace
	}
	jstr := *(*string)(unsafe.Pointer(&json))
	var res []byte
	var err error
	switch v := value.(type) {
	default:
		b, merr := jsongo.Marshal(value)
		if merr != nil {
			return nil, merr
		}
		raw := *(*string)(unsafe.Pointer(&b))
		res, err = set(jstr, path, raw, false, false, optimistic, inplace)
	case dtype:
		res, err = set(jstr, path, "", false, true, optimistic, inplace)
	case string:
		res, err = set(jstr, path, v, true, false, optimistic, inplace)
	case []byte:
		raw := *(*string)(unsafe.Pointer(&v))
		res, err = set(jstr, path, raw, true, false, optimistic, inplace)
	case bool:
		if v {
			res, err = set(jstr, path, "true", false, false, optimistic, inplace)
		} else {
			res, err = set(jstr, path, "false", false, false, optimistic, inplace)
		}
	case int8:
		res, err = set(jstr, path, strconv.FormatInt(int64(v), 10),
			false, false, optimistic, inplace)
	case int16:
		res, err = set(jstr, path, strconv.FormatInt(int64(v), 10),
			false, false, optimistic, inplace)
	case int32:
		res, err = set(jstr, path, strconv.FormatInt(int64(v), 10),
			false, false, optimistic, inplace)
	case int64:
		res, err = set(jstr, path, strconv.FormatInt(int64(v), 10),
			false, false, optimistic, inplace)
	case uint8:
		res, err = set(jstr, path, strconv.FormatUint(uint64(v), 10),
			false, false, optimistic, inplace)
	case uint16:
		res, err = set(jstr, path, strconv.FormatUint(uint64(v), 10),
			false, false, optimistic, inplace)
	case uint32:
		res, err = set(jstr, path, strconv.FormatUint(uint64(v), 10),
			false, false, optimistic, inplace)
	case uint64:
		res, err = set(jstr, path, strconv.FormatUint(uint64(v), 10),
			false, false, optimistic, inplace)
	case float32:
		res, err = set(jstr, path, strconv.FormatFloat(float64(v), 'f', -1, 64),
			false, false, optimistic, inplace)
	case float64:
		res, err = set(jstr, path, strconv.FormatFloat(float64(v), 'f', -1, 64),
			false, false, optimistic, inplace)
	}
	if err == errNoChange {
		return json, nil
	}
	return res, err
}

// SetRawBytesOptions sets a raw json value for the specified path with options.
// If working with bytes, this method preferred over
// SetRawOptions(string(data), path, value, opts)
func SetRawBytesOptions(json []byte, path string, value []byte,
	opts *Options) ([]byte, error) {
	jstr := *(*string)(unsafe.Pointer(&json))
	vstr := *(*string)(unsafe.Pointer(&value))
	var optimistic, inplace bool
	if opts != nil {
		optimistic = opts.Optimistic
		inplace = opts.ReplaceInPlace
	}
	res, err := set(jstr, path, vstr, false, false, optimistic, inplace)
	if err == errNoChange {
		return json, nil
	}
	return res, err
}
