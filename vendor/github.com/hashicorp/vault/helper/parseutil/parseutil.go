package parseutil

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/errwrap"
	sockaddr "github.com/hashicorp/go-sockaddr"
	"github.com/hashicorp/vault/helper/strutil"
	"github.com/mitchellh/mapstructure"
)

func ParseDurationSecond(in interface{}) (time.Duration, error) {
	var dur time.Duration
	jsonIn, ok := in.(json.Number)
	if ok {
		in = jsonIn.String()
	}
	switch in.(type) {
	case string:
		inp := in.(string)
		if inp == "" {
			return time.Duration(0), nil
		}
		var err error
		// Look for a suffix otherwise its a plain second value
		if strings.HasSuffix(inp, "s") || strings.HasSuffix(inp, "m") || strings.HasSuffix(inp, "h") || strings.HasSuffix(inp, "ms") {
			dur, err = time.ParseDuration(inp)
			if err != nil {
				return dur, err
			}
		} else {
			// Plain integer
			secs, err := strconv.ParseInt(inp, 10, 64)
			if err != nil {
				return dur, err
			}
			dur = time.Duration(secs) * time.Second
		}
	case int:
		dur = time.Duration(in.(int)) * time.Second
	case int32:
		dur = time.Duration(in.(int32)) * time.Second
	case int64:
		dur = time.Duration(in.(int64)) * time.Second
	case uint:
		dur = time.Duration(in.(uint)) * time.Second
	case uint32:
		dur = time.Duration(in.(uint32)) * time.Second
	case uint64:
		dur = time.Duration(in.(uint64)) * time.Second
	default:
		return 0, errors.New("could not parse duration from input")
	}

	return dur, nil
}

func ParseInt(in interface{}) (int64, error) {
	var ret int64
	jsonIn, ok := in.(json.Number)
	if ok {
		in = jsonIn.String()
	}
	switch in.(type) {
	case string:
		inp := in.(string)
		if inp == "" {
			return 0, nil
		}
		var err error
		left, err := strconv.ParseInt(inp, 10, 64)
		if err != nil {
			return ret, err
		}
		ret = left
	case int:
		ret = int64(in.(int))
	case int32:
		ret = int64(in.(int32))
	case int64:
		ret = in.(int64)
	case uint:
		ret = int64(in.(uint))
	case uint32:
		ret = int64(in.(uint32))
	case uint64:
		ret = int64(in.(uint64))
	default:
		return 0, errors.New("could not parse value from input")
	}

	return ret, nil
}

func ParseBool(in interface{}) (bool, error) {
	var result bool
	if err := mapstructure.WeakDecode(in, &result); err != nil {
		return false, err
	}
	return result, nil
}

func ParseCommaStringSlice(in interface{}) ([]string, error) {
	var result []string
	config := &mapstructure.DecoderConfig{
		Result:           &result,
		WeaklyTypedInput: true,
		DecodeHook:       mapstructure.StringToSliceHookFunc(","),
	}
	decoder, err := mapstructure.NewDecoder(config)
	if err != nil {
		return nil, err
	}
	if err := decoder.Decode(in); err != nil {
		return nil, err
	}
	return strutil.TrimStrings(result), nil
}

func ParseAddrs(addrs interface{}) ([]*sockaddr.SockAddrMarshaler, error) {
	out := make([]*sockaddr.SockAddrMarshaler, 0)
	stringAddrs := make([]string, 0)

	switch addrs.(type) {
	case string:
		stringAddrs = strutil.ParseArbitraryStringSlice(addrs.(string), ",")
		if len(stringAddrs) == 0 {
			return nil, fmt.Errorf("unable to parse addresses from %v", addrs)
		}

	case []string:
		stringAddrs = addrs.([]string)

	case []interface{}:
		for _, v := range addrs.([]interface{}) {
			stringAddr, ok := v.(string)
			if !ok {
				return nil, fmt.Errorf("error parsing %v as string", v)
			}
			stringAddrs = append(stringAddrs, stringAddr)
		}

	default:
		return nil, fmt.Errorf("unknown address input type %T", addrs)
	}

	for _, addr := range stringAddrs {
		sa, err := sockaddr.NewSockAddr(addr)
		if err != nil {
			return nil, errwrap.Wrapf(fmt.Sprintf("error parsing address %q: {{err}}", addr), err)
		}
		out = append(out, &sockaddr.SockAddrMarshaler{
			SockAddr: sa,
		})
	}

	return out, nil
}
