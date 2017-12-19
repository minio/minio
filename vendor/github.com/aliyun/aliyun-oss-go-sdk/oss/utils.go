package oss

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc64"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// Get User Agent
// Go sdk相关信息，包括sdk版本，操作系统类型，GO版本
var userAgent = func() string {
	sys := getSysInfo()
	return fmt.Sprintf("aliyun-sdk-go/%s (%s/%s/%s;%s)", Version, sys.name,
		sys.release, sys.machine, runtime.Version())
}()

type sysInfo struct {
	name    string // 操作系统名称windows/Linux
	release string // 操作系统版本 2.6.32-220.23.2.ali1089.el5.x86_64等
	machine string // 机器类型amd64/x86_64
}

// Get　system info
// 获取操作系统信息、机器类型
func getSysInfo() sysInfo {
	name := runtime.GOOS
	release := "-"
	machine := runtime.GOARCH
	if out, err := exec.Command("uname", "-s").CombinedOutput(); err == nil {
		name = string(bytes.TrimSpace(out))
	}
	if out, err := exec.Command("uname", "-r").CombinedOutput(); err == nil {
		release = string(bytes.TrimSpace(out))
	}
	if out, err := exec.Command("uname", "-m").CombinedOutput(); err == nil {
		machine = string(bytes.TrimSpace(out))
	}
	return sysInfo{name: name, release: release, machine: machine}
}

// unpackedRange
type unpackedRange struct {
	hasStart bool  // 是否指定了起点
	hasEnd   bool  // 是否指定了终点
	start    int64 // 起点
	end      int64 // 终点
}

// invalid Range Error
func invalidRangeError(r string) error {
	return fmt.Errorf("InvalidRange %s", r)
}

// parseRange parse various styles of range such as bytes=M-N
func parseRange(normalizedRange string) (*unpackedRange, error) {
	var err error
	hasStart := false
	hasEnd := false
	var start int64
	var end int64

	// bytes==M-N or ranges=M-N
	nrSlice := strings.Split(normalizedRange, "=")
	if len(nrSlice) != 2 || nrSlice[0] != "bytes" {
		return nil, invalidRangeError(normalizedRange)
	}

	// bytes=M-N,X-Y
	rSlice := strings.Split(nrSlice[1], ",")
	rStr := rSlice[0]

	if strings.HasSuffix(rStr, "-") { // M-
		startStr := rStr[:len(rStr)-1]
		start, err = strconv.ParseInt(startStr, 10, 64)
		if err != nil {
			return nil, invalidRangeError(normalizedRange)
		}
		hasStart = true
	} else if strings.HasPrefix(rStr, "-") { // -N
		len := rStr[1:]
		end, err = strconv.ParseInt(len, 10, 64)
		if err != nil {
			return nil, invalidRangeError(normalizedRange)
		}
		if end == 0 { // -0
			return nil, invalidRangeError(normalizedRange)
		}
		hasEnd = true
	} else { // M-N
		valSlice := strings.Split(rStr, "-")
		if len(valSlice) != 2 {
			return nil, invalidRangeError(normalizedRange)
		}
		start, err = strconv.ParseInt(valSlice[0], 10, 64)
		if err != nil {
			return nil, invalidRangeError(normalizedRange)
		}
		hasStart = true
		end, err = strconv.ParseInt(valSlice[1], 10, 64)
		if err != nil {
			return nil, invalidRangeError(normalizedRange)
		}
		hasEnd = true
	}

	return &unpackedRange{hasStart, hasEnd, start, end}, nil
}

// adjustRange return adjusted range, adjust the range according to the length of the file
func adjustRange(ur *unpackedRange, size int64) (start, end int64) {
	if ur == nil {
		return 0, size
	}

	if ur.hasStart && ur.hasEnd {
		start = ur.start
		end = ur.end + 1
		if ur.start < 0 || ur.start >= size || ur.end > size || ur.start > ur.end {
			start = 0
			end = size
		}
	} else if ur.hasStart {
		start = ur.start
		end = size
		if ur.start < 0 || ur.start >= size {
			start = 0
		}
	} else if ur.hasEnd {
		start = size - ur.end
		end = size
		if ur.end < 0 || ur.end > size {
			start = 0
			end = size
		}
	}
	return
}

// GetNowSec returns Unix time, the number of seconds elapsed since January 1, 1970 UTC.
// 获取当前时间，从UTC开始的秒数。
func GetNowSec() int64 {
	return time.Now().Unix()
}

// GetNowNanoSec returns t as a Unix time, the number of nanoseconds elapsed
// since January 1, 1970 UTC. The result is undefined if the Unix time
// in nanoseconds cannot be represented by an int64. Note that this
// means the result of calling UnixNano on the zero Time is undefined.
// 获取当前时间，从UTC开始的纳秒。
func GetNowNanoSec() int64 {
	return time.Now().UnixNano()
}

// GetNowGMT 获取当前时间，格式形如"Mon, 02 Jan 2006 15:04:05 GMT"，HTTP中使用的时间格式
func GetNowGMT() string {
	return time.Now().UTC().Format(http.TimeFormat)
}

// FileChunk 文件片定义
type FileChunk struct {
	Number int   // 块序号
	Offset int64 // 块在文件中的偏移量
	Size   int64 // 块大小
}

// SplitFileByPartNum Split big file to part by the num of part
// 按指定的块数分割文件。返回值FileChunk为分割结果，error为nil时有效。
func SplitFileByPartNum(fileName string, chunkNum int) ([]FileChunk, error) {
	if chunkNum <= 0 || chunkNum > 10000 {
		return nil, errors.New("chunkNum invalid")
	}

	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	if int64(chunkNum) > stat.Size() {
		return nil, errors.New("oss: chunkNum invalid")
	}

	var chunks []FileChunk
	var chunk = FileChunk{}
	var chunkN = (int64)(chunkNum)
	for i := int64(0); i < chunkN; i++ {
		chunk.Number = int(i + 1)
		chunk.Offset = i * (stat.Size() / chunkN)
		if i == chunkN-1 {
			chunk.Size = stat.Size()/chunkN + stat.Size()%chunkN
		} else {
			chunk.Size = stat.Size() / chunkN
		}
		chunks = append(chunks, chunk)
	}

	return chunks, nil
}

// SplitFileByPartSize Split big file to part by the size of part
// 按块大小分割文件。返回值FileChunk为分割结果，error为nil时有效。
func SplitFileByPartSize(fileName string, chunkSize int64) ([]FileChunk, error) {
	if chunkSize <= 0 {
		return nil, errors.New("chunkSize invalid")
	}

	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	var chunkN = stat.Size() / chunkSize
	if chunkN >= 10000 {
		return nil, errors.New("Too many parts, please increase part size.")
	}

	var chunks []FileChunk
	var chunk = FileChunk{}
	for i := int64(0); i < chunkN; i++ {
		chunk.Number = int(i + 1)
		chunk.Offset = i * chunkSize
		chunk.Size = chunkSize
		chunks = append(chunks, chunk)
	}

	if stat.Size()%chunkSize > 0 {
		chunk.Number = len(chunks) + 1
		chunk.Offset = int64(len(chunks)) * chunkSize
		chunk.Size = stat.Size() % chunkSize
		chunks = append(chunks, chunk)
	}

	return chunks, nil
}

// GetPartEnd 计算结束位置
func GetPartEnd(begin int64, total int64, per int64) int64 {
	if begin+per > total {
		return total - 1
	}
	return begin + per - 1
}

// crcTable returns the Table constructed from the specified polynomial
var crcTable = func() *crc64.Table {
	return crc64.MakeTable(crc64.ECMA)
}
