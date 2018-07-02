// +build linux

package process

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/host"
	"github.com/shirou/gopsutil/internal/common"
	"github.com/shirou/gopsutil/net"
	"golang.org/x/sys/unix"
)

var PageSize = uint64(os.Getpagesize())

const (
	PrioProcess = 0   // linux/resource.h
	ClockTicks  = 100 // C.sysconf(C._SC_CLK_TCK)
)

// MemoryInfoExStat is different between OSes
type MemoryInfoExStat struct {
	RSS    uint64 `json:"rss"`    // bytes
	VMS    uint64 `json:"vms"`    // bytes
	Shared uint64 `json:"shared"` // bytes
	Text   uint64 `json:"text"`   // bytes
	Lib    uint64 `json:"lib"`    // bytes
	Data   uint64 `json:"data"`   // bytes
	Dirty  uint64 `json:"dirty"`  // bytes
}

func (m MemoryInfoExStat) String() string {
	s, _ := json.Marshal(m)
	return string(s)
}

type MemoryMapsStat struct {
	Path         string `json:"path"`
	Rss          uint64 `json:"rss"`
	Size         uint64 `json:"size"`
	Pss          uint64 `json:"pss"`
	SharedClean  uint64 `json:"sharedClean"`
	SharedDirty  uint64 `json:"sharedDirty"`
	PrivateClean uint64 `json:"privateClean"`
	PrivateDirty uint64 `json:"privateDirty"`
	Referenced   uint64 `json:"referenced"`
	Anonymous    uint64 `json:"anonymous"`
	Swap         uint64 `json:"swap"`
}

// String returns JSON value of the process.
func (m MemoryMapsStat) String() string {
	s, _ := json.Marshal(m)
	return string(s)
}

// NewProcess creates a new Process instance, it only stores the pid and
// checks that the process exists. Other method on Process can be used
// to get more information about the process. An error will be returned
// if the process does not exist.
func NewProcess(pid int32) (*Process, error) {
	p := &Process{
		Pid: int32(pid),
	}
	file, err := os.Open(common.HostProc(strconv.Itoa(int(p.Pid))))
	defer file.Close()
	return p, err
}

// Ppid returns Parent Process ID of the process.
func (p *Process) Ppid() (int32, error) {
	return p.PpidWithContext(context.Background())
}

func (p *Process) PpidWithContext(ctx context.Context) (int32, error) {
	_, ppid, _, _, _, _, err := p.fillFromStat()
	if err != nil {
		return -1, err
	}
	return ppid, nil
}

// Name returns name of the process.
func (p *Process) Name() (string, error) {
	return p.NameWithContext(context.Background())
}

func (p *Process) NameWithContext(ctx context.Context) (string, error) {
	if p.name == "" {
		if err := p.fillFromStatus(); err != nil {
			return "", err
		}
	}
	return p.name, nil
}

// Tgid returns tgid, a Linux-synonym for user-space Pid
func (p *Process) Tgid() (int32, error) {
	if p.tgid == 0 {
		if err := p.fillFromStatus(); err != nil {
			return 0, err
		}
	}
	return p.tgid, nil
}

// Exe returns executable path of the process.
func (p *Process) Exe() (string, error) {
	return p.ExeWithContext(context.Background())
}

func (p *Process) ExeWithContext(ctx context.Context) (string, error) {
	return p.fillFromExe()
}

// Cmdline returns the command line arguments of the process as a string with
// each argument separated by 0x20 ascii character.
func (p *Process) Cmdline() (string, error) {
	return p.CmdlineWithContext(context.Background())
}

func (p *Process) CmdlineWithContext(ctx context.Context) (string, error) {
	return p.fillFromCmdline()
}

// CmdlineSlice returns the command line arguments of the process as a slice with each
// element being an argument.
func (p *Process) CmdlineSlice() ([]string, error) {
	return p.CmdlineSliceWithContext(context.Background())
}

func (p *Process) CmdlineSliceWithContext(ctx context.Context) ([]string, error) {
	return p.fillSliceFromCmdline()
}

// CreateTime returns created time of the process in milliseconds since the epoch, in UTC.
func (p *Process) CreateTime() (int64, error) {
	return p.CreateTimeWithContext(context.Background())
}

func (p *Process) CreateTimeWithContext(ctx context.Context) (int64, error) {
	_, _, _, createTime, _, _, err := p.fillFromStat()
	if err != nil {
		return 0, err
	}
	return createTime, nil
}

// Cwd returns current working directory of the process.
func (p *Process) Cwd() (string, error) {
	return p.CwdWithContext(context.Background())
}

func (p *Process) CwdWithContext(ctx context.Context) (string, error) {
	return p.fillFromCwd()
}

// Parent returns parent Process of the process.
func (p *Process) Parent() (*Process, error) {
	return p.ParentWithContext(context.Background())
}

func (p *Process) ParentWithContext(ctx context.Context) (*Process, error) {
	err := p.fillFromStatus()
	if err != nil {
		return nil, err
	}
	if p.parent == 0 {
		return nil, fmt.Errorf("wrong number of parents")
	}
	return NewProcess(p.parent)
}

// Status returns the process status.
// Return value could be one of these.
// R: Running S: Sleep T: Stop I: Idle
// Z: Zombie W: Wait L: Lock
// The charactor is same within all supported platforms.
func (p *Process) Status() (string, error) {
	return p.StatusWithContext(context.Background())
}

func (p *Process) StatusWithContext(ctx context.Context) (string, error) {
	err := p.fillFromStatus()
	if err != nil {
		return "", err
	}
	return p.status, nil
}

// Uids returns user ids of the process as a slice of the int
func (p *Process) Uids() ([]int32, error) {
	return p.UidsWithContext(context.Background())
}

func (p *Process) UidsWithContext(ctx context.Context) ([]int32, error) {
	err := p.fillFromStatus()
	if err != nil {
		return []int32{}, err
	}
	return p.uids, nil
}

// Gids returns group ids of the process as a slice of the int
func (p *Process) Gids() ([]int32, error) {
	return p.GidsWithContext(context.Background())
}

func (p *Process) GidsWithContext(ctx context.Context) ([]int32, error) {
	err := p.fillFromStatus()
	if err != nil {
		return []int32{}, err
	}
	return p.gids, nil
}

// Terminal returns a terminal which is associated with the process.
func (p *Process) Terminal() (string, error) {
	return p.TerminalWithContext(context.Background())
}

func (p *Process) TerminalWithContext(ctx context.Context) (string, error) {
	t, _, _, _, _, _, err := p.fillFromStat()
	if err != nil {
		return "", err
	}
	termmap, err := getTerminalMap()
	if err != nil {
		return "", err
	}
	terminal := termmap[t]
	return terminal, nil
}

// Nice returns a nice value (priority).
// Notice: gopsutil can not set nice value.
func (p *Process) Nice() (int32, error) {
	return p.NiceWithContext(context.Background())
}

func (p *Process) NiceWithContext(ctx context.Context) (int32, error) {
	_, _, _, _, _, nice, err := p.fillFromStat()
	if err != nil {
		return 0, err
	}
	return nice, nil
}

// IOnice returns process I/O nice value (priority).
func (p *Process) IOnice() (int32, error) {
	return p.IOniceWithContext(context.Background())
}

func (p *Process) IOniceWithContext(ctx context.Context) (int32, error) {
	return 0, common.ErrNotImplementedError
}

// Rlimit returns Resource Limits.
func (p *Process) Rlimit() ([]RlimitStat, error) {
	return p.RlimitWithContext(context.Background())
}

func (p *Process) RlimitWithContext(ctx context.Context) ([]RlimitStat, error) {
	return p.RlimitUsage(false)
}

// RlimitUsage returns Resource Limits.
// If gatherUsed is true, the currently used value will be gathered and added
// to the resulting RlimitStat.
func (p *Process) RlimitUsage(gatherUsed bool) ([]RlimitStat, error) {
	return p.RlimitUsageWithContext(context.Background(), gatherUsed)
}

func (p *Process) RlimitUsageWithContext(ctx context.Context, gatherUsed bool) ([]RlimitStat, error) {
	rlimits, err := p.fillFromLimits()
	if !gatherUsed || err != nil {
		return rlimits, err
	}

	_, _, _, _, rtprio, nice, err := p.fillFromStat()
	if err != nil {
		return nil, err
	}
	if err := p.fillFromStatus(); err != nil {
		return nil, err
	}

	for i := range rlimits {
		rs := &rlimits[i]
		switch rs.Resource {
		case RLIMIT_CPU:
			times, err := p.Times()
			if err != nil {
				return nil, err
			}
			rs.Used = uint64(times.User + times.System)
		case RLIMIT_DATA:
			rs.Used = uint64(p.memInfo.Data)
		case RLIMIT_STACK:
			rs.Used = uint64(p.memInfo.Stack)
		case RLIMIT_RSS:
			rs.Used = uint64(p.memInfo.RSS)
		case RLIMIT_NOFILE:
			n, err := p.NumFDs()
			if err != nil {
				return nil, err
			}
			rs.Used = uint64(n)
		case RLIMIT_MEMLOCK:
			rs.Used = uint64(p.memInfo.Locked)
		case RLIMIT_AS:
			rs.Used = uint64(p.memInfo.VMS)
		case RLIMIT_LOCKS:
			//TODO we can get the used value from /proc/$pid/locks. But linux doesn't enforce it, so not a high priority.
		case RLIMIT_SIGPENDING:
			rs.Used = p.sigInfo.PendingProcess
		case RLIMIT_NICE:
			// The rlimit for nice is a little unusual, in that 0 means the niceness cannot be decreased beyond the current value, but it can be increased.
			// So effectively: if rs.Soft == 0 { rs.Soft = rs.Used }
			rs.Used = uint64(nice)
		case RLIMIT_RTPRIO:
			rs.Used = uint64(rtprio)
		}
	}

	return rlimits, err
}

// IOCounters returns IO Counters.
func (p *Process) IOCounters() (*IOCountersStat, error) {
	return p.IOCountersWithContext(context.Background())
}

func (p *Process) IOCountersWithContext(ctx context.Context) (*IOCountersStat, error) {
	return p.fillFromIO()
}

// NumCtxSwitches returns the number of the context switches of the process.
func (p *Process) NumCtxSwitches() (*NumCtxSwitchesStat, error) {
	return p.NumCtxSwitchesWithContext(context.Background())
}

func (p *Process) NumCtxSwitchesWithContext(ctx context.Context) (*NumCtxSwitchesStat, error) {
	err := p.fillFromStatus()
	if err != nil {
		return nil, err
	}
	return p.numCtxSwitches, nil
}

// NumFDs returns the number of File Descriptors used by the process.
func (p *Process) NumFDs() (int32, error) {
	return p.NumFDsWithContext(context.Background())
}

func (p *Process) NumFDsWithContext(ctx context.Context) (int32, error) {
	_, fnames, err := p.fillFromfdList()
	return int32(len(fnames)), err
}

// NumThreads returns the number of threads used by the process.
func (p *Process) NumThreads() (int32, error) {
	return p.NumThreadsWithContext(context.Background())
}

func (p *Process) NumThreadsWithContext(ctx context.Context) (int32, error) {
	err := p.fillFromStatus()
	if err != nil {
		return 0, err
	}
	return p.numThreads, nil
}

func (p *Process) Threads() (map[int32]*cpu.TimesStat, error) {
	return p.ThreadsWithContext(context.Background())
}

func (p *Process) ThreadsWithContext(ctx context.Context) (map[int32]*cpu.TimesStat, error) {
	ret := make(map[int32]*cpu.TimesStat)
	taskPath := common.HostProc(strconv.Itoa(int(p.Pid)), "task")

	tids, err := readPidsFromDir(taskPath)
	if err != nil {
		return nil, err
	}

	for _, tid := range tids {
		_, _, cpuTimes, _, _, _, err := p.fillFromTIDStat(tid)
		if err != nil {
			return nil, err
		}
		ret[tid] = cpuTimes
	}

	return ret, nil
}

// Times returns CPU times of the process.
func (p *Process) Times() (*cpu.TimesStat, error) {
	return p.TimesWithContext(context.Background())
}

func (p *Process) TimesWithContext(ctx context.Context) (*cpu.TimesStat, error) {
	_, _, cpuTimes, _, _, _, err := p.fillFromStat()
	if err != nil {
		return nil, err
	}
	return cpuTimes, nil
}

// CPUAffinity returns CPU affinity of the process.
//
// Notice: Not implemented yet.
func (p *Process) CPUAffinity() ([]int32, error) {
	return p.CPUAffinityWithContext(context.Background())
}

func (p *Process) CPUAffinityWithContext(ctx context.Context) ([]int32, error) {
	return nil, common.ErrNotImplementedError
}

// MemoryInfo returns platform in-dependend memory information, such as RSS, VMS and Swap
func (p *Process) MemoryInfo() (*MemoryInfoStat, error) {
	return p.MemoryInfoWithContext(context.Background())
}

func (p *Process) MemoryInfoWithContext(ctx context.Context) (*MemoryInfoStat, error) {
	meminfo, _, err := p.fillFromStatm()
	if err != nil {
		return nil, err
	}
	return meminfo, nil
}

// MemoryInfoEx returns platform dependend memory information.
func (p *Process) MemoryInfoEx() (*MemoryInfoExStat, error) {
	return p.MemoryInfoExWithContext(context.Background())
}

func (p *Process) MemoryInfoExWithContext(ctx context.Context) (*MemoryInfoExStat, error) {
	_, memInfoEx, err := p.fillFromStatm()
	if err != nil {
		return nil, err
	}
	return memInfoEx, nil
}

// Children returns a slice of Process of the process.
func (p *Process) Children() ([]*Process, error) {
	return p.ChildrenWithContext(context.Background())
}

func (p *Process) ChildrenWithContext(ctx context.Context) ([]*Process, error) {
	pids, err := common.CallPgrepWithContext(ctx, invoke, p.Pid)
	if err != nil {
		if pids == nil || len(pids) == 0 {
			return nil, ErrorNoChildren
		}
		return nil, err
	}
	ret := make([]*Process, 0, len(pids))
	for _, pid := range pids {
		np, err := NewProcess(pid)
		if err != nil {
			return nil, err
		}
		ret = append(ret, np)
	}
	return ret, nil
}

// OpenFiles returns a slice of OpenFilesStat opend by the process.
// OpenFilesStat includes a file path and file descriptor.
func (p *Process) OpenFiles() ([]OpenFilesStat, error) {
	return p.OpenFilesWithContext(context.Background())
}

func (p *Process) OpenFilesWithContext(ctx context.Context) ([]OpenFilesStat, error) {
	_, ofs, err := p.fillFromfd()
	if err != nil {
		return nil, err
	}
	ret := make([]OpenFilesStat, len(ofs))
	for i, o := range ofs {
		ret[i] = *o
	}

	return ret, nil
}

// Connections returns a slice of net.ConnectionStat used by the process.
// This returns all kind of the connection. This measn TCP, UDP or UNIX.
func (p *Process) Connections() ([]net.ConnectionStat, error) {
	return p.ConnectionsWithContext(context.Background())
}

func (p *Process) ConnectionsWithContext(ctx context.Context) ([]net.ConnectionStat, error) {
	return net.ConnectionsPid("all", p.Pid)
}

// NetIOCounters returns NetIOCounters of the process.
func (p *Process) NetIOCounters(pernic bool) ([]net.IOCountersStat, error) {
	return p.NetIOCountersWithContext(context.Background(), pernic)
}

func (p *Process) NetIOCountersWithContext(ctx context.Context, pernic bool) ([]net.IOCountersStat, error) {
	filename := common.HostProc(strconv.Itoa(int(p.Pid)), "net/dev")
	return net.IOCountersByFile(pernic, filename)
}

// IsRunning returns whether the process is running or not.
// Not implemented yet.
func (p *Process) IsRunning() (bool, error) {
	return p.IsRunningWithContext(context.Background())
}

func (p *Process) IsRunningWithContext(ctx context.Context) (bool, error) {
	return true, common.ErrNotImplementedError
}

// MemoryMaps get memory maps from /proc/(pid)/smaps
func (p *Process) MemoryMaps(grouped bool) (*[]MemoryMapsStat, error) {
	return p.MemoryMapsWithContext(context.Background(), grouped)
}

func (p *Process) MemoryMapsWithContext(ctx context.Context, grouped bool) (*[]MemoryMapsStat, error) {
	pid := p.Pid
	var ret []MemoryMapsStat
	smapsPath := common.HostProc(strconv.Itoa(int(pid)), "smaps")
	contents, err := ioutil.ReadFile(smapsPath)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(contents), "\n")

	// function of parsing a block
	getBlock := func(first_line []string, block []string) (MemoryMapsStat, error) {
		m := MemoryMapsStat{}
		m.Path = first_line[len(first_line)-1]

		for _, line := range block {
			if strings.Contains(line, "VmFlags") {
				continue
			}
			field := strings.Split(line, ":")
			if len(field) < 2 {
				continue
			}
			v := strings.Trim(field[1], " kB") // remove last "kB"
			t, err := strconv.ParseUint(v, 10, 64)
			if err != nil {
				return m, err
			}

			switch field[0] {
			case "Size":
				m.Size = t
			case "Rss":
				m.Rss = t
			case "Pss":
				m.Pss = t
			case "Shared_Clean":
				m.SharedClean = t
			case "Shared_Dirty":
				m.SharedDirty = t
			case "Private_Clean":
				m.PrivateClean = t
			case "Private_Dirty":
				m.PrivateDirty = t
			case "Referenced":
				m.Referenced = t
			case "Anonymous":
				m.Anonymous = t
			case "Swap":
				m.Swap = t
			}
		}
		return m, nil
	}

	blocks := make([]string, 16)
	for _, line := range lines {
		field := strings.Split(line, " ")
		if strings.HasSuffix(field[0], ":") == false {
			// new block section
			if len(blocks) > 0 {
				g, err := getBlock(field, blocks)
				if err != nil {
					return &ret, err
				}
				ret = append(ret, g)
			}
			// starts new block
			blocks = make([]string, 16)
		} else {
			blocks = append(blocks, line)
		}
	}

	return &ret, nil
}

/**
** Internal functions
**/

func limitToInt(val string) (int32, error) {
	if val == "unlimited" {
		return math.MaxInt32, nil
	} else {
		res, err := strconv.ParseInt(val, 10, 32)
		if err != nil {
			return 0, err
		}
		return int32(res), nil
	}
}

// Get num_fds from /proc/(pid)/limits
func (p *Process) fillFromLimits() ([]RlimitStat, error) {
	return p.fillFromLimitsWithContext(context.Background())
}

func (p *Process) fillFromLimitsWithContext(ctx context.Context) ([]RlimitStat, error) {
	pid := p.Pid
	limitsFile := common.HostProc(strconv.Itoa(int(pid)), "limits")
	d, err := os.Open(limitsFile)
	if err != nil {
		return nil, err
	}
	defer d.Close()

	var limitStats []RlimitStat

	limitsScanner := bufio.NewScanner(d)
	for limitsScanner.Scan() {
		var statItem RlimitStat

		str := strings.Fields(limitsScanner.Text())

		// Remove the header line
		if strings.Contains(str[len(str)-1], "Units") {
			continue
		}

		// Assert that last item is a Hard limit
		statItem.Hard, err = limitToInt(str[len(str)-1])
		if err != nil {
			// On error remove last item an try once again since it can be unit or header line
			str = str[:len(str)-1]
			statItem.Hard, err = limitToInt(str[len(str)-1])
			if err != nil {
				return nil, err
			}
		}
		// Remove last item from string
		str = str[:len(str)-1]

		//Now last item is a Soft limit
		statItem.Soft, err = limitToInt(str[len(str)-1])
		if err != nil {
			return nil, err
		}
		// Remove last item from string
		str = str[:len(str)-1]

		//The rest is a stats name
		resourceName := strings.Join(str, " ")
		switch resourceName {
		case "Max cpu time":
			statItem.Resource = RLIMIT_CPU
		case "Max file size":
			statItem.Resource = RLIMIT_FSIZE
		case "Max data size":
			statItem.Resource = RLIMIT_DATA
		case "Max stack size":
			statItem.Resource = RLIMIT_STACK
		case "Max core file size":
			statItem.Resource = RLIMIT_CORE
		case "Max resident set":
			statItem.Resource = RLIMIT_RSS
		case "Max processes":
			statItem.Resource = RLIMIT_NPROC
		case "Max open files":
			statItem.Resource = RLIMIT_NOFILE
		case "Max locked memory":
			statItem.Resource = RLIMIT_MEMLOCK
		case "Max address space":
			statItem.Resource = RLIMIT_AS
		case "Max file locks":
			statItem.Resource = RLIMIT_LOCKS
		case "Max pending signals":
			statItem.Resource = RLIMIT_SIGPENDING
		case "Max msgqueue size":
			statItem.Resource = RLIMIT_MSGQUEUE
		case "Max nice priority":
			statItem.Resource = RLIMIT_NICE
		case "Max realtime priority":
			statItem.Resource = RLIMIT_RTPRIO
		case "Max realtime timeout":
			statItem.Resource = RLIMIT_RTTIME
		default:
			continue
		}

		limitStats = append(limitStats, statItem)
	}

	if err := limitsScanner.Err(); err != nil {
		return nil, err
	}

	return limitStats, nil
}

// Get list of /proc/(pid)/fd files
func (p *Process) fillFromfdList() (string, []string, error) {
	return p.fillFromfdListWithContext(context.Background())
}

func (p *Process) fillFromfdListWithContext(ctx context.Context) (string, []string, error) {
	pid := p.Pid
	statPath := common.HostProc(strconv.Itoa(int(pid)), "fd")
	d, err := os.Open(statPath)
	if err != nil {
		return statPath, []string{}, err
	}
	defer d.Close()
	fnames, err := d.Readdirnames(-1)
	return statPath, fnames, err
}

// Get num_fds from /proc/(pid)/fd
func (p *Process) fillFromfd() (int32, []*OpenFilesStat, error) {
	return p.fillFromfdWithContext(context.Background())
}

func (p *Process) fillFromfdWithContext(ctx context.Context) (int32, []*OpenFilesStat, error) {
	statPath, fnames, err := p.fillFromfdList()
	if err != nil {
		return 0, nil, err
	}
	numFDs := int32(len(fnames))

	var openfiles []*OpenFilesStat
	for _, fd := range fnames {
		fpath := filepath.Join(statPath, fd)
		filepath, err := os.Readlink(fpath)
		if err != nil {
			continue
		}
		t, err := strconv.ParseUint(fd, 10, 64)
		if err != nil {
			return numFDs, openfiles, err
		}
		o := &OpenFilesStat{
			Path: filepath,
			Fd:   t,
		}
		openfiles = append(openfiles, o)
	}

	return numFDs, openfiles, nil
}

// Get cwd from /proc/(pid)/cwd
func (p *Process) fillFromCwd() (string, error) {
	return p.fillFromCwdWithContext(context.Background())
}

func (p *Process) fillFromCwdWithContext(ctx context.Context) (string, error) {
	pid := p.Pid
	cwdPath := common.HostProc(strconv.Itoa(int(pid)), "cwd")
	cwd, err := os.Readlink(cwdPath)
	if err != nil {
		return "", err
	}
	return string(cwd), nil
}

// Get exe from /proc/(pid)/exe
func (p *Process) fillFromExe() (string, error) {
	return p.fillFromExeWithContext(context.Background())
}

func (p *Process) fillFromExeWithContext(ctx context.Context) (string, error) {
	pid := p.Pid
	exePath := common.HostProc(strconv.Itoa(int(pid)), "exe")
	exe, err := os.Readlink(exePath)
	if err != nil {
		return "", err
	}
	return string(exe), nil
}

// Get cmdline from /proc/(pid)/cmdline
func (p *Process) fillFromCmdline() (string, error) {
	return p.fillFromCmdlineWithContext(context.Background())
}

func (p *Process) fillFromCmdlineWithContext(ctx context.Context) (string, error) {
	pid := p.Pid
	cmdPath := common.HostProc(strconv.Itoa(int(pid)), "cmdline")
	cmdline, err := ioutil.ReadFile(cmdPath)
	if err != nil {
		return "", err
	}
	ret := strings.FieldsFunc(string(cmdline), func(r rune) bool {
		if r == '\u0000' {
			return true
		}
		return false
	})

	return strings.Join(ret, " "), nil
}

func (p *Process) fillSliceFromCmdline() ([]string, error) {
	return p.fillSliceFromCmdlineWithContext(context.Background())
}

func (p *Process) fillSliceFromCmdlineWithContext(ctx context.Context) ([]string, error) {
	pid := p.Pid
	cmdPath := common.HostProc(strconv.Itoa(int(pid)), "cmdline")
	cmdline, err := ioutil.ReadFile(cmdPath)
	if err != nil {
		return nil, err
	}
	if len(cmdline) == 0 {
		return nil, nil
	}
	if cmdline[len(cmdline)-1] == 0 {
		cmdline = cmdline[:len(cmdline)-1]
	}
	parts := bytes.Split(cmdline, []byte{0})
	var strParts []string
	for _, p := range parts {
		strParts = append(strParts, string(p))
	}

	return strParts, nil
}

// Get IO status from /proc/(pid)/io
func (p *Process) fillFromIO() (*IOCountersStat, error) {
	return p.fillFromIOWithContext(context.Background())
}

func (p *Process) fillFromIOWithContext(ctx context.Context) (*IOCountersStat, error) {
	pid := p.Pid
	ioPath := common.HostProc(strconv.Itoa(int(pid)), "io")
	ioline, err := ioutil.ReadFile(ioPath)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(ioline), "\n")
	ret := &IOCountersStat{}

	for _, line := range lines {
		field := strings.Fields(line)
		if len(field) < 2 {
			continue
		}
		t, err := strconv.ParseUint(field[1], 10, 64)
		if err != nil {
			return nil, err
		}
		param := field[0]
		if strings.HasSuffix(param, ":") {
			param = param[:len(param)-1]
		}
		switch param {
		case "syscr":
			ret.ReadCount = t
		case "syscw":
			ret.WriteCount = t
		case "read_bytes":
			ret.ReadBytes = t
		case "write_bytes":
			ret.WriteBytes = t
		}
	}

	return ret, nil
}

// Get memory info from /proc/(pid)/statm
func (p *Process) fillFromStatm() (*MemoryInfoStat, *MemoryInfoExStat, error) {
	return p.fillFromStatmWithContext(context.Background())
}

func (p *Process) fillFromStatmWithContext(ctx context.Context) (*MemoryInfoStat, *MemoryInfoExStat, error) {
	pid := p.Pid
	memPath := common.HostProc(strconv.Itoa(int(pid)), "statm")
	contents, err := ioutil.ReadFile(memPath)
	if err != nil {
		return nil, nil, err
	}
	fields := strings.Split(string(contents), " ")

	vms, err := strconv.ParseUint(fields[0], 10, 64)
	if err != nil {
		return nil, nil, err
	}
	rss, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return nil, nil, err
	}
	memInfo := &MemoryInfoStat{
		RSS: rss * PageSize,
		VMS: vms * PageSize,
	}

	shared, err := strconv.ParseUint(fields[2], 10, 64)
	if err != nil {
		return nil, nil, err
	}
	text, err := strconv.ParseUint(fields[3], 10, 64)
	if err != nil {
		return nil, nil, err
	}
	lib, err := strconv.ParseUint(fields[4], 10, 64)
	if err != nil {
		return nil, nil, err
	}
	dirty, err := strconv.ParseUint(fields[5], 10, 64)
	if err != nil {
		return nil, nil, err
	}

	memInfoEx := &MemoryInfoExStat{
		RSS:    rss * PageSize,
		VMS:    vms * PageSize,
		Shared: shared * PageSize,
		Text:   text * PageSize,
		Lib:    lib * PageSize,
		Dirty:  dirty * PageSize,
	}

	return memInfo, memInfoEx, nil
}

// Get various status from /proc/(pid)/status
func (p *Process) fillFromStatus() error {
	return p.fillFromStatusWithContext(context.Background())
}

func (p *Process) fillFromStatusWithContext(ctx context.Context) error {
	pid := p.Pid
	statPath := common.HostProc(strconv.Itoa(int(pid)), "status")
	contents, err := ioutil.ReadFile(statPath)
	if err != nil {
		return err
	}
	lines := strings.Split(string(contents), "\n")
	p.numCtxSwitches = &NumCtxSwitchesStat{}
	p.memInfo = &MemoryInfoStat{}
	p.sigInfo = &SignalInfoStat{}
	for _, line := range lines {
		tabParts := strings.SplitN(line, "\t", 2)
		if len(tabParts) < 2 {
			continue
		}
		value := tabParts[1]
		switch strings.TrimRight(tabParts[0], ":") {
		case "Name":
			p.name = strings.Trim(value, " \t")
			if len(p.name) >= 15 {
				cmdlineSlice, err := p.CmdlineSlice()
				if err != nil {
					return err
				}
				if len(cmdlineSlice) > 0 {
					extendedName := filepath.Base(cmdlineSlice[0])
					if strings.HasPrefix(extendedName, p.name) {
						p.name = extendedName
					}
				}
			}
		case "State":
			p.status = value[0:1]
		case "PPid", "Ppid":
			pval, err := strconv.ParseInt(value, 10, 32)
			if err != nil {
				return err
			}
			p.parent = int32(pval)
		case "Tgid":
			pval, err := strconv.ParseInt(value, 10, 32)
			if err != nil {
				return err
			}
			p.tgid = int32(pval)
		case "Uid":
			p.uids = make([]int32, 0, 4)
			for _, i := range strings.Split(value, "\t") {
				v, err := strconv.ParseInt(i, 10, 32)
				if err != nil {
					return err
				}
				p.uids = append(p.uids, int32(v))
			}
		case "Gid":
			p.gids = make([]int32, 0, 4)
			for _, i := range strings.Split(value, "\t") {
				v, err := strconv.ParseInt(i, 10, 32)
				if err != nil {
					return err
				}
				p.gids = append(p.gids, int32(v))
			}
		case "Threads":
			v, err := strconv.ParseInt(value, 10, 32)
			if err != nil {
				return err
			}
			p.numThreads = int32(v)
		case "voluntary_ctxt_switches":
			v, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return err
			}
			p.numCtxSwitches.Voluntary = v
		case "nonvoluntary_ctxt_switches":
			v, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return err
			}
			p.numCtxSwitches.Involuntary = v
		case "VmRSS":
			value := strings.Trim(value, " kB") // remove last "kB"
			v, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				return err
			}
			p.memInfo.RSS = v * 1024
		case "VmSize":
			value := strings.Trim(value, " kB") // remove last "kB"
			v, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				return err
			}
			p.memInfo.VMS = v * 1024
		case "VmSwap":
			value := strings.Trim(value, " kB") // remove last "kB"
			v, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				return err
			}
			p.memInfo.Swap = v * 1024
		case "VmData":
			value := strings.Trim(value, " kB") // remove last "kB"
			v, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				return err
			}
			p.memInfo.Data = v * 1024
		case "VmStk":
			value := strings.Trim(value, " kB") // remove last "kB"
			v, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				return err
			}
			p.memInfo.Stack = v * 1024
		case "VmLck":
			value := strings.Trim(value, " kB") // remove last "kB"
			v, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				return err
			}
			p.memInfo.Locked = v * 1024
		case "SigPnd":
			v, err := strconv.ParseUint(value, 16, 64)
			if err != nil {
				return err
			}
			p.sigInfo.PendingThread = v
		case "ShdPnd":
			v, err := strconv.ParseUint(value, 16, 64)
			if err != nil {
				return err
			}
			p.sigInfo.PendingProcess = v
		case "SigBlk":
			v, err := strconv.ParseUint(value, 16, 64)
			if err != nil {
				return err
			}
			p.sigInfo.Blocked = v
		case "SigIgn":
			v, err := strconv.ParseUint(value, 16, 64)
			if err != nil {
				return err
			}
			p.sigInfo.Ignored = v
		case "SigCgt":
			v, err := strconv.ParseUint(value, 16, 64)
			if err != nil {
				return err
			}
			p.sigInfo.Caught = v
		}

	}
	return nil
}

func (p *Process) fillFromTIDStat(tid int32) (uint64, int32, *cpu.TimesStat, int64, uint32, int32, error) {
	return p.fillFromTIDStatWithContext(context.Background(), tid)
}

func (p *Process) fillFromTIDStatWithContext(ctx context.Context, tid int32) (uint64, int32, *cpu.TimesStat, int64, uint32, int32, error) {
	pid := p.Pid
	var statPath string

	if tid == -1 {
		statPath = common.HostProc(strconv.Itoa(int(pid)), "stat")
	} else {
		statPath = common.HostProc(strconv.Itoa(int(pid)), "task", strconv.Itoa(int(tid)), "stat")
	}

	contents, err := ioutil.ReadFile(statPath)
	if err != nil {
		return 0, 0, nil, 0, 0, 0, err
	}
	fields := strings.Fields(string(contents))

	i := 1
	for !strings.HasSuffix(fields[i], ")") {
		i++
	}

	terminal, err := strconv.ParseUint(fields[i+5], 10, 64)
	if err != nil {
		return 0, 0, nil, 0, 0, 0, err
	}

	ppid, err := strconv.ParseInt(fields[i+2], 10, 32)
	if err != nil {
		return 0, 0, nil, 0, 0, 0, err
	}
	utime, err := strconv.ParseFloat(fields[i+12], 64)
	if err != nil {
		return 0, 0, nil, 0, 0, 0, err
	}

	stime, err := strconv.ParseFloat(fields[i+13], 64)
	if err != nil {
		return 0, 0, nil, 0, 0, 0, err
	}

	cpuTimes := &cpu.TimesStat{
		CPU:    "cpu",
		User:   float64(utime / ClockTicks),
		System: float64(stime / ClockTicks),
	}

	bootTime, _ := host.BootTime()
	t, err := strconv.ParseUint(fields[i+20], 10, 64)
	if err != nil {
		return 0, 0, nil, 0, 0, 0, err
	}
	ctime := (t / uint64(ClockTicks)) + uint64(bootTime)
	createTime := int64(ctime * 1000)

	rtpriority, err := strconv.ParseInt(fields[i+16], 10, 32)
	if rtpriority < 0 {
		rtpriority = rtpriority*-1 - 1
	} else {
		rtpriority = 0
	}

	//	p.Nice = mustParseInt32(fields[18])
	// use syscall instead of parse Stat file
	snice, _ := unix.Getpriority(PrioProcess, int(pid))
	nice := int32(snice) // FIXME: is this true?

	return terminal, int32(ppid), cpuTimes, createTime, uint32(rtpriority), nice, nil
}

func (p *Process) fillFromStat() (uint64, int32, *cpu.TimesStat, int64, uint32, int32, error) {
	return p.fillFromStatWithContext(context.Background())
}

func (p *Process) fillFromStatWithContext(ctx context.Context) (uint64, int32, *cpu.TimesStat, int64, uint32, int32, error) {
	return p.fillFromTIDStat(-1)
}

// Pids returns a slice of process ID list which are running now.
func Pids() ([]int32, error) {
	return PidsWithContext(context.Background())
}

func PidsWithContext(ctx context.Context) ([]int32, error) {
	return readPidsFromDir(common.HostProc())
}

// Process returns a slice of pointers to Process structs for all
// currently running processes.
func Processes() ([]*Process, error) {
	return ProcessesWithContext(context.Background())
}

func ProcessesWithContext(ctx context.Context) ([]*Process, error) {
	out := []*Process{}

	pids, err := Pids()
	if err != nil {
		return out, err
	}

	for _, pid := range pids {
		p, err := NewProcess(pid)
		if err != nil {
			continue
		}
		out = append(out, p)
	}

	return out, nil
}

func readPidsFromDir(path string) ([]int32, error) {
	var ret []int32

	d, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer d.Close()

	fnames, err := d.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	for _, fname := range fnames {
		pid, err := strconv.ParseInt(fname, 10, 32)
		if err != nil {
			// if not numeric name, just skip
			continue
		}
		ret = append(ret, int32(pid))
	}

	return ret, nil
}
