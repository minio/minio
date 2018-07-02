// +build windows

package process

import (
	"context"
	"fmt"
	"os"
	"strings"
	"syscall"
	"time"
	"unsafe"

	"github.com/StackExchange/wmi"
	cpu "github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/internal/common"
	net "github.com/shirou/gopsutil/net"
	"github.com/shirou/w32"
	"golang.org/x/sys/windows"
)

const (
	NoMoreFiles   = 0x12
	MaxPathLength = 260
)

var (
	modpsapi                 = windows.NewLazyDLL("psapi.dll")
	procGetProcessMemoryInfo = modpsapi.NewProc("GetProcessMemoryInfo")
)

type SystemProcessInformation struct {
	NextEntryOffset   uint64
	NumberOfThreads   uint64
	Reserved1         [48]byte
	Reserved2         [3]byte
	UniqueProcessID   uintptr
	Reserved3         uintptr
	HandleCount       uint64
	Reserved4         [4]byte
	Reserved5         [11]byte
	PeakPagefileUsage uint64
	PrivatePageCount  uint64
	Reserved6         [6]uint64
}

// Memory_info_ex is different between OSes
type MemoryInfoExStat struct {
}

type MemoryMapsStat struct {
}

type Win32_Process struct {
	Name                  string
	ExecutablePath        *string
	CommandLine           *string
	Priority              uint32
	CreationDate          *time.Time
	ProcessID             uint32
	ThreadCount           uint32
	Status                *string
	ReadOperationCount    uint64
	ReadTransferCount     uint64
	WriteOperationCount   uint64
	WriteTransferCount    uint64
	CSCreationClassName   string
	CSName                string
	Caption               *string
	CreationClassName     string
	Description           *string
	ExecutionState        *uint16
	HandleCount           uint32
	KernelModeTime        uint64
	MaximumWorkingSetSize *uint32
	MinimumWorkingSetSize *uint32
	OSCreationClassName   string
	OSName                string
	OtherOperationCount   uint64
	OtherTransferCount    uint64
	PageFaults            uint32
	PageFileUsage         uint32
	ParentProcessID       uint32
	PeakPageFileUsage     uint32
	PeakVirtualSize       uint64
	PeakWorkingSetSize    uint32
	PrivatePageCount      uint64
	TerminationDate       *time.Time
	UserModeTime          uint64
	WorkingSetSize        uint64
}

func init() {
	wmi.DefaultClient.AllowMissingFields = true
}

func Pids() ([]int32, error) {
	return PidsWithContext(context.Background())
}

func PidsWithContext(ctx context.Context) ([]int32, error) {
	// inspired by https://gist.github.com/henkman/3083408
	// and https://github.com/giampaolo/psutil/blob/1c3a15f637521ba5c0031283da39c733fda53e4c/psutil/arch/windows/process_info.c#L315-L329
	var ret []int32
	var read uint32 = 0
	var psSize uint32 = 1024
	const dwordSize uint32 = 4

	for {
		ps := make([]uint32, psSize)
		if !w32.EnumProcesses(ps, uint32(len(ps)), &read) {
			return nil, fmt.Errorf("could not get w32.EnumProcesses")
		}
		if uint32(len(ps)) == read { // ps buffer was too small to host every results, retry with a bigger one
			psSize += 1024
			continue
		}
		for _, pid := range ps[:read/dwordSize] {
			ret = append(ret, int32(pid))
		}
		return ret, nil

	}

}

func (p *Process) Ppid() (int32, error) {
	return p.PpidWithContext(context.Background())
}

func (p *Process) PpidWithContext(ctx context.Context) (int32, error) {
	ppid, _, _, err := getFromSnapProcess(p.Pid)
	if err != nil {
		return 0, err
	}
	return ppid, nil
}

func GetWin32Proc(pid int32) ([]Win32_Process, error) {
	return GetWin32ProcWithContext(context.Background(), pid)
}

func GetWin32ProcWithContext(ctx context.Context, pid int32) ([]Win32_Process, error) {
	var dst []Win32_Process
	query := fmt.Sprintf("WHERE ProcessId = %d", pid)
	q := wmi.CreateQuery(&dst, query)
	err := common.WMIQueryWithContext(ctx, q, &dst)
	if err != nil {
		return []Win32_Process{}, fmt.Errorf("could not get win32Proc: %s", err)
	}

	if len(dst) == 0 {
		return []Win32_Process{}, fmt.Errorf("could not get win32Proc: empty")
	}

	return dst, nil
}

func (p *Process) Name() (string, error) {
	return p.NameWithContext(context.Background())
}

func (p *Process) NameWithContext(ctx context.Context) (string, error) {
	_, _, name, err := getFromSnapProcess(p.Pid)
	if err != nil {
		return "", fmt.Errorf("could not get Name: %s", err)
	}
	return name, nil
}

func (p *Process) Tgid() (int32, error) {
	return 0, common.ErrNotImplementedError
}

func (p *Process) Exe() (string, error) {
	return p.ExeWithContext(context.Background())
}

func (p *Process) ExeWithContext(ctx context.Context) (string, error) {
	dst, err := GetWin32Proc(p.Pid)
	if err != nil {
		return "", fmt.Errorf("could not get ExecutablePath: %s", err)
	}
	return *dst[0].ExecutablePath, nil
}

func (p *Process) Cmdline() (string, error) {
	return p.CmdlineWithContext(context.Background())
}

func (p *Process) CmdlineWithContext(ctx context.Context) (string, error) {
	dst, err := GetWin32Proc(p.Pid)
	if err != nil {
		return "", fmt.Errorf("could not get CommandLine: %s", err)
	}
	return *dst[0].CommandLine, nil
}

// CmdlineSlice returns the command line arguments of the process as a slice with each
// element being an argument. This merely returns the CommandLine informations passed
// to the process split on the 0x20 ASCII character.
func (p *Process) CmdlineSlice() ([]string, error) {
	return p.CmdlineSliceWithContext(context.Background())
}

func (p *Process) CmdlineSliceWithContext(ctx context.Context) ([]string, error) {
	cmdline, err := p.Cmdline()
	if err != nil {
		return nil, err
	}
	return strings.Split(cmdline, " "), nil
}

func (p *Process) CreateTime() (int64, error) {
	return p.CreateTimeWithContext(context.Background())
}

func (p *Process) CreateTimeWithContext(ctx context.Context) (int64, error) {
	ru, err := getRusage(p.Pid)
	if err != nil {
		return 0, fmt.Errorf("could not get CreationDate: %s", err)
	}

	return ru.CreationTime.Nanoseconds() / 1000000, nil
}

func (p *Process) Cwd() (string, error) {
	return p.CwdWithContext(context.Background())
}

func (p *Process) CwdWithContext(ctx context.Context) (string, error) {
	return "", common.ErrNotImplementedError
}
func (p *Process) Parent() (*Process, error) {
	return p.ParentWithContext(context.Background())
}

func (p *Process) ParentWithContext(ctx context.Context) (*Process, error) {
	dst, err := GetWin32Proc(p.Pid)
	if err != nil {
		return nil, fmt.Errorf("could not get ParentProcessID: %s", err)
	}

	return NewProcess(int32(dst[0].ParentProcessID))
}
func (p *Process) Status() (string, error) {
	return p.StatusWithContext(context.Background())
}

func (p *Process) StatusWithContext(ctx context.Context) (string, error) {
	return "", common.ErrNotImplementedError
}
func (p *Process) Username() (string, error) {
	return p.UsernameWithContext(context.Background())
}

func (p *Process) UsernameWithContext(ctx context.Context) (string, error) {
	pid := p.Pid
	// 0x1000 is PROCESS_QUERY_LIMITED_INFORMATION
	c, err := syscall.OpenProcess(0x1000, false, uint32(pid))
	if err != nil {
		return "", err
	}
	defer syscall.CloseHandle(c)

	var token syscall.Token
	err = syscall.OpenProcessToken(c, syscall.TOKEN_QUERY, &token)
	if err != nil {
		return "", err
	}
	defer token.Close()
	tokenUser, err := token.GetTokenUser()

	user, domain, _, err := tokenUser.User.Sid.LookupAccount("")
	return domain + "\\" + user, err
}

func (p *Process) Uids() ([]int32, error) {
	return p.UidsWithContext(context.Background())
}

func (p *Process) UidsWithContext(ctx context.Context) ([]int32, error) {
	var uids []int32

	return uids, common.ErrNotImplementedError
}
func (p *Process) Gids() ([]int32, error) {
	return p.GidsWithContext(context.Background())
}

func (p *Process) GidsWithContext(ctx context.Context) ([]int32, error) {
	var gids []int32
	return gids, common.ErrNotImplementedError
}
func (p *Process) Terminal() (string, error) {
	return p.TerminalWithContext(context.Background())
}

func (p *Process) TerminalWithContext(ctx context.Context) (string, error) {
	return "", common.ErrNotImplementedError
}

// Nice returnes priority in Windows
func (p *Process) Nice() (int32, error) {
	return p.NiceWithContext(context.Background())
}

func (p *Process) NiceWithContext(ctx context.Context) (int32, error) {
	dst, err := GetWin32Proc(p.Pid)
	if err != nil {
		return 0, fmt.Errorf("could not get Priority: %s", err)
	}
	return int32(dst[0].Priority), nil
}
func (p *Process) IOnice() (int32, error) {
	return p.IOniceWithContext(context.Background())
}

func (p *Process) IOniceWithContext(ctx context.Context) (int32, error) {
	return 0, common.ErrNotImplementedError
}
func (p *Process) Rlimit() ([]RlimitStat, error) {
	return p.RlimitWithContext(context.Background())
}

func (p *Process) RlimitWithContext(ctx context.Context) ([]RlimitStat, error) {
	var rlimit []RlimitStat

	return rlimit, common.ErrNotImplementedError
}
func (p *Process) RlimitUsage(gatherUsed bool) ([]RlimitStat, error) {
	return p.RlimitUsageWithContext(context.Background(), gatherUsed)
}

func (p *Process) RlimitUsageWithContext(ctx context.Context, gatherUsed bool) ([]RlimitStat, error) {
	var rlimit []RlimitStat

	return rlimit, common.ErrNotImplementedError
}

func (p *Process) IOCounters() (*IOCountersStat, error) {
	return p.IOCountersWithContext(context.Background())
}

func (p *Process) IOCountersWithContext(ctx context.Context) (*IOCountersStat, error) {
	dst, err := GetWin32Proc(p.Pid)
	if err != nil || len(dst) == 0 {
		return nil, fmt.Errorf("could not get Win32Proc: %s", err)
	}
	ret := &IOCountersStat{
		ReadCount:  uint64(dst[0].ReadOperationCount),
		ReadBytes:  uint64(dst[0].ReadTransferCount),
		WriteCount: uint64(dst[0].WriteOperationCount),
		WriteBytes: uint64(dst[0].WriteTransferCount),
	}

	return ret, nil
}
func (p *Process) NumCtxSwitches() (*NumCtxSwitchesStat, error) {
	return p.NumCtxSwitchesWithContext(context.Background())
}

func (p *Process) NumCtxSwitchesWithContext(ctx context.Context) (*NumCtxSwitchesStat, error) {
	return nil, common.ErrNotImplementedError
}
func (p *Process) NumFDs() (int32, error) {
	return p.NumFDsWithContext(context.Background())
}

func (p *Process) NumFDsWithContext(ctx context.Context) (int32, error) {
	return 0, common.ErrNotImplementedError
}
func (p *Process) NumThreads() (int32, error) {
	return p.NumThreadsWithContext(context.Background())
}

func (p *Process) NumThreadsWithContext(ctx context.Context) (int32, error) {
	dst, err := GetWin32Proc(p.Pid)
	if err != nil {
		return 0, fmt.Errorf("could not get ThreadCount: %s", err)
	}
	return int32(dst[0].ThreadCount), nil
}
func (p *Process) Threads() (map[int32]*cpu.TimesStat, error) {
	return p.ThreadsWithContext(context.Background())
}

func (p *Process) ThreadsWithContext(ctx context.Context) (map[int32]*cpu.TimesStat, error) {
	ret := make(map[int32]*cpu.TimesStat)
	return ret, common.ErrNotImplementedError
}
func (p *Process) Times() (*cpu.TimesStat, error) {
	return p.TimesWithContext(context.Background())
}

func (p *Process) TimesWithContext(ctx context.Context) (*cpu.TimesStat, error) {
	sysTimes, err := getProcessCPUTimes(p.Pid)
	if err != nil {
		return nil, err
	}

	// User and kernel times are represented as a FILETIME structure
	// which contains a 64-bit value representing the number of
	// 100-nanosecond intervals since January 1, 1601 (UTC):
	// http://msdn.microsoft.com/en-us/library/ms724284(VS.85).aspx
	// To convert it into a float representing the seconds that the
	// process has executed in user/kernel mode I borrowed the code
	// below from psutil's _psutil_windows.c, and in turn from Python's
	// Modules/posixmodule.c

	user := float64(sysTimes.UserTime.HighDateTime)*429.4967296 + float64(sysTimes.UserTime.LowDateTime)*1e-7
	kernel := float64(sysTimes.KernelTime.HighDateTime)*429.4967296 + float64(sysTimes.KernelTime.LowDateTime)*1e-7

	return &cpu.TimesStat{
		User:   user,
		System: kernel,
	}, nil
}
func (p *Process) CPUAffinity() ([]int32, error) {
	return p.CPUAffinityWithContext(context.Background())
}

func (p *Process) CPUAffinityWithContext(ctx context.Context) ([]int32, error) {
	return nil, common.ErrNotImplementedError
}
func (p *Process) MemoryInfo() (*MemoryInfoStat, error) {
	return p.MemoryInfoWithContext(context.Background())
}

func (p *Process) MemoryInfoWithContext(ctx context.Context) (*MemoryInfoStat, error) {
	mem, err := getMemoryInfo(p.Pid)
	if err != nil {
		return nil, err
	}

	ret := &MemoryInfoStat{
		RSS: uint64(mem.WorkingSetSize),
		VMS: uint64(mem.PagefileUsage),
	}

	return ret, nil
}
func (p *Process) MemoryInfoEx() (*MemoryInfoExStat, error) {
	return p.MemoryInfoExWithContext(context.Background())
}

func (p *Process) MemoryInfoExWithContext(ctx context.Context) (*MemoryInfoExStat, error) {
	return nil, common.ErrNotImplementedError
}

func (p *Process) Children() ([]*Process, error) {
	return p.ChildrenWithContext(context.Background())
}

func (p *Process) ChildrenWithContext(ctx context.Context) ([]*Process, error) {
	var dst []Win32_Process
	query := wmi.CreateQuery(&dst, fmt.Sprintf("Where ParentProcessId = %d", p.Pid))
	err := common.WMIQueryWithContext(ctx, query, &dst)
	if err != nil {
		return nil, err
	}

	out := []*Process{}
	for _, proc := range dst {
		p, err := NewProcess(int32(proc.ProcessID))
		if err != nil {
			continue
		}
		out = append(out, p)
	}

	return out, nil
}

func (p *Process) OpenFiles() ([]OpenFilesStat, error) {
	return p.OpenFilesWithContext(context.Background())
}

func (p *Process) OpenFilesWithContext(ctx context.Context) ([]OpenFilesStat, error) {
	return nil, common.ErrNotImplementedError
}

func (p *Process) Connections() ([]net.ConnectionStat, error) {
	return p.ConnectionsWithContext(context.Background())
}

func (p *Process) ConnectionsWithContext(ctx context.Context) ([]net.ConnectionStat, error) {
	return nil, common.ErrNotImplementedError
}

func (p *Process) NetIOCounters(pernic bool) ([]net.IOCountersStat, error) {
	return p.NetIOCountersWithContext(context.Background(), pernic)
}

func (p *Process) NetIOCountersWithContext(ctx context.Context, pernic bool) ([]net.IOCountersStat, error) {
	return nil, common.ErrNotImplementedError
}

func (p *Process) IsRunning() (bool, error) {
	return p.IsRunningWithContext(context.Background())
}

func (p *Process) IsRunningWithContext(ctx context.Context) (bool, error) {
	return true, common.ErrNotImplementedError
}

func (p *Process) MemoryMaps(grouped bool) (*[]MemoryMapsStat, error) {
	return p.MemoryMapsWithContext(context.Background(), grouped)
}

func (p *Process) MemoryMapsWithContext(ctx context.Context, grouped bool) (*[]MemoryMapsStat, error) {
	var ret []MemoryMapsStat
	return &ret, common.ErrNotImplementedError
}

func NewProcess(pid int32) (*Process, error) {
	p := &Process{Pid: pid}

	return p, nil
}

func (p *Process) SendSignal(sig windows.Signal) error {
	return p.SendSignalWithContext(context.Background(), sig)
}

func (p *Process) SendSignalWithContext(ctx context.Context, sig windows.Signal) error {
	return common.ErrNotImplementedError
}

func (p *Process) Suspend() error {
	return p.SuspendWithContext(context.Background())
}

func (p *Process) SuspendWithContext(ctx context.Context) error {
	return common.ErrNotImplementedError
}
func (p *Process) Resume() error {
	return p.ResumeWithContext(context.Background())
}

func (p *Process) ResumeWithContext(ctx context.Context) error {
	return common.ErrNotImplementedError
}

func (p *Process) Terminate() error {
	return p.TerminateWithContext(context.Background())
}

func (p *Process) TerminateWithContext(ctx context.Context) error {
	// PROCESS_TERMINATE = 0x0001
	proc := w32.OpenProcess(0x0001, false, uint32(p.Pid))
	ret := w32.TerminateProcess(proc, 0)
	w32.CloseHandle(proc)

	if ret == false {
		return windows.GetLastError()
	} else {
		return nil
	}
}

func (p *Process) Kill() error {
	return p.KillWithContext(context.Background())
}

func (p *Process) KillWithContext(ctx context.Context) error {
	process := os.Process{Pid: int(p.Pid)}
	return process.Kill()
}

func getFromSnapProcess(pid int32) (int32, int32, string, error) {
	snap := w32.CreateToolhelp32Snapshot(w32.TH32CS_SNAPPROCESS, uint32(pid))
	if snap == 0 {
		return 0, 0, "", windows.GetLastError()
	}
	defer w32.CloseHandle(snap)
	var pe32 w32.PROCESSENTRY32
	pe32.DwSize = uint32(unsafe.Sizeof(pe32))
	if w32.Process32First(snap, &pe32) == false {
		return 0, 0, "", windows.GetLastError()
	}

	if pe32.Th32ProcessID == uint32(pid) {
		szexe := windows.UTF16ToString(pe32.SzExeFile[:])
		return int32(pe32.Th32ParentProcessID), int32(pe32.CntThreads), szexe, nil
	}

	for w32.Process32Next(snap, &pe32) {
		if pe32.Th32ProcessID == uint32(pid) {
			szexe := windows.UTF16ToString(pe32.SzExeFile[:])
			return int32(pe32.Th32ParentProcessID), int32(pe32.CntThreads), szexe, nil
		}
	}
	return 0, 0, "", fmt.Errorf("Couldn't find pid: %d", pid)
}

// Get processes
func Processes() ([]*Process, error) {
	return ProcessesWithContext(context.Background())
}

func ProcessesWithContext(ctx context.Context) ([]*Process, error) {
	pids, err := Pids()
	if err != nil {
		return []*Process{}, fmt.Errorf("could not get Processes %s", err)
	}

	results := []*Process{}
	for _, pid := range pids {
		p, err := NewProcess(int32(pid))
		if err != nil {
			continue
		}
		results = append(results, p)
	}

	return results, nil
}

func getProcInfo(pid int32) (*SystemProcessInformation, error) {
	initialBufferSize := uint64(0x4000)
	bufferSize := initialBufferSize
	buffer := make([]byte, bufferSize)

	var sysProcInfo SystemProcessInformation
	ret, _, _ := common.ProcNtQuerySystemInformation.Call(
		uintptr(unsafe.Pointer(&sysProcInfo)),
		uintptr(unsafe.Pointer(&buffer[0])),
		uintptr(unsafe.Pointer(&bufferSize)),
		uintptr(unsafe.Pointer(&bufferSize)))
	if ret != 0 {
		return nil, windows.GetLastError()
	}

	return &sysProcInfo, nil
}

func getRusage(pid int32) (*windows.Rusage, error) {
	var CPU windows.Rusage

	c, err := windows.OpenProcess(windows.PROCESS_QUERY_INFORMATION, false, uint32(pid))
	if err != nil {
		return nil, err
	}
	defer windows.CloseHandle(c)

	if err := windows.GetProcessTimes(c, &CPU.CreationTime, &CPU.ExitTime, &CPU.KernelTime, &CPU.UserTime); err != nil {
		return nil, err
	}

	return &CPU, nil
}

func getMemoryInfo(pid int32) (PROCESS_MEMORY_COUNTERS, error) {
	var mem PROCESS_MEMORY_COUNTERS
	// PROCESS_QUERY_LIMITED_INFORMATION is 0x1000
	c, err := windows.OpenProcess(0x1000, false, uint32(pid))
	if err != nil {
		return mem, err
	}
	defer windows.CloseHandle(c)
	if err := getProcessMemoryInfo(c, &mem); err != nil {
		return mem, err
	}

	return mem, err
}

func getProcessMemoryInfo(h windows.Handle, mem *PROCESS_MEMORY_COUNTERS) (err error) {
	r1, _, e1 := syscall.Syscall(procGetProcessMemoryInfo.Addr(), 3, uintptr(h), uintptr(unsafe.Pointer(mem)), uintptr(unsafe.Sizeof(*mem)))
	if r1 == 0 {
		if e1 != 0 {
			err = error(e1)
		} else {
			err = syscall.EINVAL
		}
	}
	return
}

type SYSTEM_TIMES struct {
	CreateTime syscall.Filetime
	ExitTime   syscall.Filetime
	KernelTime syscall.Filetime
	UserTime   syscall.Filetime
}

func getProcessCPUTimes(pid int32) (SYSTEM_TIMES, error) {
	var times SYSTEM_TIMES

	// PROCESS_QUERY_LIMITED_INFORMATION is 0x1000
	h, err := windows.OpenProcess(0x1000, false, uint32(pid))
	if err != nil {
		return times, err
	}
	defer windows.CloseHandle(h)

	err = syscall.GetProcessTimes(
		syscall.Handle(h),
		&times.CreateTime,
		&times.ExitTime,
		&times.KernelTime,
		&times.UserTime,
	)

	return times, err
}
