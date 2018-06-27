// +build darwin
// +build cgo

package disk

/*
#cgo LDFLAGS: -lobjc -framework Foundation -framework IOKit
#include <stdint.h>

// ### enough?
const int MAX_DISK_NAME = 100;

typedef struct
{
    char DiskName[MAX_DISK_NAME];
    int64_t Reads;
    int64_t Writes;
    int64_t ReadBytes;
    int64_t WriteBytes;
    int64_t ReadTime;
    int64_t WriteTime;
} DiskInfo;

#include "disk_darwin.h"
*/
import "C"

import (
	"context"
	"errors"
	"strings"
	"unsafe"

	"github.com/shirou/gopsutil/internal/common"
)

func IOCounters(names ...string) (map[string]IOCountersStat, error) {
	return IOCountersWithContext(context.Background(), names...)
}

func IOCountersWithContext(ctx context.Context, names ...string) (map[string]IOCountersStat, error) {
	if C.StartIOCounterFetch() == 0 {
		return nil, errors.New("Unable to fetch disk list")
	}

	// Clean up when we are done.
	defer C.EndIOCounterFetch()
	ret := make(map[string]IOCountersStat, 0)

	for {
		res := C.FetchNextDisk()
		if res == -1 {
			return nil, errors.New("Unable to fetch disk information")
		} else if res == 0 {
			break // done
		}

		di := C.DiskInfo{}
		if C.ReadDiskInfo((*C.DiskInfo)(unsafe.Pointer(&di))) == -1 {
			return nil, errors.New("Unable to fetch disk properties")
		}

		// Used to only get the necessary part of the C string.
		isRuneNull := func(r rune) bool {
			return r == '\u0000'
		}

		// Map from the darwin-specific C struct to the Go type
		//
		// ### missing: IopsInProgress, WeightedIO, MergedReadCount,
		// MergedWriteCount, SerialNumber
		// IOKit can give us at least the serial number I think...
		d := IOCountersStat{
			// Note: The Go type wants unsigned values, but CFNumberGetValue
			// doesn't appear to be able to give us unsigned values. So, we
			// cast, and hope for the best.
			ReadBytes:  uint64(di.ReadBytes),
			WriteBytes: uint64(di.WriteBytes),
			ReadCount:  uint64(di.Reads),
			WriteCount: uint64(di.Writes),
			ReadTime:   uint64(di.ReadTime),
			WriteTime:  uint64(di.WriteTime),
			IoTime:     uint64(di.ReadTime + di.WriteTime),
			Name:       strings.TrimFunc(C.GoStringN(&di.DiskName[0], C.MAX_DISK_NAME), isRuneNull),
		}

		if len(names) > 0 && !common.StringsHas(names, d.Name) {
			continue
		}

		ret[d.Name] = d
	}

	return ret, nil
}
