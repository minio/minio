// +build linux

package net

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"strconv"
	"strings"
	"syscall"

	"github.com/shirou/gopsutil/internal/common"
)

// NetIOCounters returnes network I/O statistics for every network
// interface installed on the system.  If pernic argument is false,
// return only sum of all information (which name is 'all'). If true,
// every network interface installed on the system is returned
// separately.
func IOCounters(pernic bool) ([]IOCountersStat, error) {
	return IOCountersWithContext(context.Background(), pernic)
}

func IOCountersWithContext(ctx context.Context, pernic bool) ([]IOCountersStat, error) {
	filename := common.HostProc("net/dev")
	return IOCountersByFile(pernic, filename)
}

func IOCountersByFile(pernic bool, filename string) ([]IOCountersStat, error) {
	return IOCountersByFileWithContext(context.Background(), pernic, filename)
}

func IOCountersByFileWithContext(ctx context.Context, pernic bool, filename string) ([]IOCountersStat, error) {
	lines, err := common.ReadLines(filename)
	if err != nil {
		return nil, err
	}

	parts := make([]string, 2)

	statlen := len(lines) - 1

	ret := make([]IOCountersStat, 0, statlen)

	for _, line := range lines[2:] {
		separatorPos := strings.LastIndex(line, ":")
		if separatorPos == -1 {
			continue
		}
		parts[0] = line[0:separatorPos]
		parts[1] = line[separatorPos+1:]

		interfaceName := strings.TrimSpace(parts[0])
		if interfaceName == "" {
			continue
		}

		fields := strings.Fields(strings.TrimSpace(parts[1]))
		bytesRecv, err := strconv.ParseUint(fields[0], 10, 64)
		if err != nil {
			return ret, err
		}
		packetsRecv, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return ret, err
		}
		errIn, err := strconv.ParseUint(fields[2], 10, 64)
		if err != nil {
			return ret, err
		}
		dropIn, err := strconv.ParseUint(fields[3], 10, 64)
		if err != nil {
			return ret, err
		}
		fifoIn, err := strconv.ParseUint(fields[4], 10, 64)
		if err != nil {
			return ret, err
		}
		bytesSent, err := strconv.ParseUint(fields[8], 10, 64)
		if err != nil {
			return ret, err
		}
		packetsSent, err := strconv.ParseUint(fields[9], 10, 64)
		if err != nil {
			return ret, err
		}
		errOut, err := strconv.ParseUint(fields[10], 10, 64)
		if err != nil {
			return ret, err
		}
		dropOut, err := strconv.ParseUint(fields[11], 10, 64)
		if err != nil {
			return ret, err
		}
		fifoOut, err := strconv.ParseUint(fields[12], 10, 64)
		if err != nil {
			return ret, err
		}

		nic := IOCountersStat{
			Name:        interfaceName,
			BytesRecv:   bytesRecv,
			PacketsRecv: packetsRecv,
			Errin:       errIn,
			Dropin:      dropIn,
			Fifoin:      fifoIn,
			BytesSent:   bytesSent,
			PacketsSent: packetsSent,
			Errout:      errOut,
			Dropout:     dropOut,
			Fifoout:     fifoOut,
		}
		ret = append(ret, nic)
	}

	if pernic == false {
		return getIOCountersAll(ret)
	}

	return ret, nil
}

var netProtocols = []string{
	"ip",
	"icmp",
	"icmpmsg",
	"tcp",
	"udp",
	"udplite",
}

// NetProtoCounters returns network statistics for the entire system
// If protocols is empty then all protocols are returned, otherwise
// just the protocols in the list are returned.
// Available protocols:
//   ip,icmp,icmpmsg,tcp,udp,udplite
func ProtoCounters(protocols []string) ([]ProtoCountersStat, error) {
	return ProtoCountersWithContext(context.Background(), protocols)
}

func ProtoCountersWithContext(ctx context.Context, protocols []string) ([]ProtoCountersStat, error) {
	if len(protocols) == 0 {
		protocols = netProtocols
	}

	stats := make([]ProtoCountersStat, 0, len(protocols))
	protos := make(map[string]bool, len(protocols))
	for _, p := range protocols {
		protos[p] = true
	}

	filename := common.HostProc("net/snmp")
	lines, err := common.ReadLines(filename)
	if err != nil {
		return nil, err
	}

	linecount := len(lines)
	for i := 0; i < linecount; i++ {
		line := lines[i]
		r := strings.IndexRune(line, ':')
		if r == -1 {
			return nil, errors.New(filename + " is not fomatted correctly, expected ':'.")
		}
		proto := strings.ToLower(line[:r])
		if !protos[proto] {
			// skip protocol and data line
			i++
			continue
		}

		// Read header line
		statNames := strings.Split(line[r+2:], " ")

		// Read data line
		i++
		statValues := strings.Split(lines[i][r+2:], " ")
		if len(statNames) != len(statValues) {
			return nil, errors.New(filename + " is not fomatted correctly, expected same number of columns.")
		}
		stat := ProtoCountersStat{
			Protocol: proto,
			Stats:    make(map[string]int64, len(statNames)),
		}
		for j := range statNames {
			value, err := strconv.ParseInt(statValues[j], 10, 64)
			if err != nil {
				return nil, err
			}
			stat.Stats[statNames[j]] = value
		}
		stats = append(stats, stat)
	}
	return stats, nil
}

// NetFilterCounters returns iptables conntrack statistics
// the currently in use conntrack count and the max.
// If the file does not exist or is invalid it will return nil.
func FilterCounters() ([]FilterStat, error) {
	return FilterCountersWithContext(context.Background())
}

func FilterCountersWithContext(ctx context.Context) ([]FilterStat, error) {
	countfile := common.HostProc("sys/net/netfilter/nf_conntrack_count")
	maxfile := common.HostProc("sys/net/netfilter/nf_conntrack_max")

	count, err := common.ReadInts(countfile)

	if err != nil {
		return nil, err
	}
	stats := make([]FilterStat, 0, 1)

	max, err := common.ReadInts(maxfile)
	if err != nil {
		return nil, err
	}

	payload := FilterStat{
		ConnTrackCount: count[0],
		ConnTrackMax:   max[0],
	}

	stats = append(stats, payload)
	return stats, nil
}

// http://students.mimuw.edu.pl/lxr/source/include/net/tcp_states.h
var TCPStatuses = map[string]string{
	"01": "ESTABLISHED",
	"02": "SYN_SENT",
	"03": "SYN_RECV",
	"04": "FIN_WAIT1",
	"05": "FIN_WAIT2",
	"06": "TIME_WAIT",
	"07": "CLOSE",
	"08": "CLOSE_WAIT",
	"09": "LAST_ACK",
	"0A": "LISTEN",
	"0B": "CLOSING",
}

type netConnectionKindType struct {
	family   uint32
	sockType uint32
	filename string
}

var kindTCP4 = netConnectionKindType{
	family:   syscall.AF_INET,
	sockType: syscall.SOCK_STREAM,
	filename: "tcp",
}
var kindTCP6 = netConnectionKindType{
	family:   syscall.AF_INET6,
	sockType: syscall.SOCK_STREAM,
	filename: "tcp6",
}
var kindUDP4 = netConnectionKindType{
	family:   syscall.AF_INET,
	sockType: syscall.SOCK_DGRAM,
	filename: "udp",
}
var kindUDP6 = netConnectionKindType{
	family:   syscall.AF_INET6,
	sockType: syscall.SOCK_DGRAM,
	filename: "udp6",
}
var kindUNIX = netConnectionKindType{
	family:   syscall.AF_UNIX,
	filename: "unix",
}

var netConnectionKindMap = map[string][]netConnectionKindType{
	"all":   {kindTCP4, kindTCP6, kindUDP4, kindUDP6, kindUNIX},
	"tcp":   {kindTCP4, kindTCP6},
	"tcp4":  {kindTCP4},
	"tcp6":  {kindTCP6},
	"udp":   {kindUDP4, kindUDP6},
	"udp4":  {kindUDP4},
	"udp6":  {kindUDP6},
	"unix":  {kindUNIX},
	"inet":  {kindTCP4, kindTCP6, kindUDP4, kindUDP6},
	"inet4": {kindTCP4, kindUDP4},
	"inet6": {kindTCP6, kindUDP6},
}

type inodeMap struct {
	pid int32
	fd  uint32
}

type connTmp struct {
	fd       uint32
	family   uint32
	sockType uint32
	laddr    Addr
	raddr    Addr
	status   string
	pid      int32
	boundPid int32
	path     string
}

// Return a list of network connections opened.
func Connections(kind string) ([]ConnectionStat, error) {
	return ConnectionsWithContext(context.Background(), kind)
}

func ConnectionsWithContext(ctx context.Context, kind string) ([]ConnectionStat, error) {
	return ConnectionsPid(kind, 0)
}

// Return a list of network connections opened returning at most `max`
// connections for each running process.
func ConnectionsMax(kind string, max int) ([]ConnectionStat, error) {
	return ConnectionsMaxWithContext(context.Background(), kind, max)
}

func ConnectionsMaxWithContext(ctx context.Context, kind string, max int) ([]ConnectionStat, error) {
	return ConnectionsPidMax(kind, 0, max)
}

// Return a list of network connections opened by a process.
func ConnectionsPid(kind string, pid int32) ([]ConnectionStat, error) {
	return ConnectionsPidWithContext(context.Background(), kind, pid)
}

func ConnectionsPidWithContext(ctx context.Context, kind string, pid int32) ([]ConnectionStat, error) {
	tmap, ok := netConnectionKindMap[kind]
	if !ok {
		return nil, fmt.Errorf("invalid kind, %s", kind)
	}
	root := common.HostProc()
	var err error
	var inodes map[string][]inodeMap
	if pid == 0 {
		inodes, err = getProcInodesAll(root, 0)
	} else {
		inodes, err = getProcInodes(root, pid, 0)
		if len(inodes) == 0 {
			// no connection for the pid
			return []ConnectionStat{}, nil
		}
	}
	if err != nil {
		return nil, fmt.Errorf("cound not get pid(s), %d: %s", pid, err)
	}
	return statsFromInodes(root, pid, tmap, inodes)
}

// Return up to `max` network connections opened by a process.
func ConnectionsPidMax(kind string, pid int32, max int) ([]ConnectionStat, error) {
	return ConnectionsPidMaxWithContext(context.Background(), kind, pid, max)
}

func ConnectionsPidMaxWithContext(ctx context.Context, kind string, pid int32, max int) ([]ConnectionStat, error) {
	tmap, ok := netConnectionKindMap[kind]
	if !ok {
		return nil, fmt.Errorf("invalid kind, %s", kind)
	}
	root := common.HostProc()
	var err error
	var inodes map[string][]inodeMap
	if pid == 0 {
		inodes, err = getProcInodesAll(root, max)
	} else {
		inodes, err = getProcInodes(root, pid, max)
		if len(inodes) == 0 {
			// no connection for the pid
			return []ConnectionStat{}, nil
		}
	}
	if err != nil {
		return nil, fmt.Errorf("cound not get pid(s), %d", pid)
	}
	return statsFromInodes(root, pid, tmap, inodes)
}

func statsFromInodes(root string, pid int32, tmap []netConnectionKindType, inodes map[string][]inodeMap) ([]ConnectionStat, error) {
	dupCheckMap := make(map[string]struct{})
	var ret []ConnectionStat

	var err error
	for _, t := range tmap {
		var path string
		var connKey string
		var ls []connTmp
		path = fmt.Sprintf("%s/net/%s", root, t.filename)
		switch t.family {
		case syscall.AF_INET:
			fallthrough
		case syscall.AF_INET6:
			ls, err = processInet(path, t, inodes, pid)
		case syscall.AF_UNIX:
			ls, err = processUnix(path, t, inodes, pid)
		}
		if err != nil {
			return nil, err
		}
		for _, c := range ls {
			// Build TCP key to id the connection uniquely
			// socket type, src ip, src port, dst ip, dst port and state should be enough
			// to prevent duplications.
			connKey = fmt.Sprintf("%d-%s:%d-%s:%d-%s", c.sockType, c.laddr.IP, c.laddr.Port, c.raddr.IP, c.raddr.Port, c.status)
			if _, ok := dupCheckMap[connKey]; ok {
				continue
			}

			conn := ConnectionStat{
				Fd:     c.fd,
				Family: c.family,
				Type:   c.sockType,
				Laddr:  c.laddr,
				Raddr:  c.raddr,
				Status: c.status,
				Pid:    c.pid,
			}
			if c.pid == 0 {
				conn.Pid = c.boundPid
			} else {
				conn.Pid = c.pid
			}

			// fetch process owner Real, effective, saved set, and filesystem UIDs
			proc := process{Pid: conn.Pid}
			conn.Uids, _ = proc.getUids()

			ret = append(ret, conn)
			dupCheckMap[connKey] = struct{}{}
		}

	}

	return ret, nil
}

// getProcInodes returnes fd of the pid.
func getProcInodes(root string, pid int32, max int) (map[string][]inodeMap, error) {
	ret := make(map[string][]inodeMap)

	dir := fmt.Sprintf("%s/%d/fd", root, pid)
	f, err := os.Open(dir)
	if err != nil {
		return ret, err
	}
	defer f.Close()
	files, err := f.Readdir(max)
	if err != nil {
		return ret, err
	}
	for _, fd := range files {
		inodePath := fmt.Sprintf("%s/%d/fd/%s", root, pid, fd.Name())

		inode, err := os.Readlink(inodePath)
		if err != nil {
			continue
		}
		if !strings.HasPrefix(inode, "socket:[") {
			continue
		}
		// the process is using a socket
		l := len(inode)
		inode = inode[8 : l-1]
		_, ok := ret[inode]
		if !ok {
			ret[inode] = make([]inodeMap, 0)
		}
		fd, err := strconv.Atoi(fd.Name())
		if err != nil {
			continue
		}

		i := inodeMap{
			pid: pid,
			fd:  uint32(fd),
		}
		ret[inode] = append(ret[inode], i)
	}
	return ret, nil
}

// Pids retunres all pids.
// Note: this is a copy of process_linux.Pids()
// FIXME: Import process occures import cycle.
// move to common made other platform breaking. Need consider.
func Pids() ([]int32, error) {
	return PidsWithContext(context.Background())
}

func PidsWithContext(ctx context.Context) ([]int32, error) {
	var ret []int32

	d, err := os.Open(common.HostProc())
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

// Note: the following is based off process_linux structs and methods
// we need these to fetch the owner of a process ID
// FIXME: Import process occures import cycle.
// see remarks on pids()
type process struct {
	Pid  int32 `json:"pid"`
	uids []int32
}

// Uids returns user ids of the process as a slice of the int
func (p *process) getUids() ([]int32, error) {
	err := p.fillFromStatus()
	if err != nil {
		return []int32{}, err
	}
	return p.uids, nil
}

// Get status from /proc/(pid)/status
func (p *process) fillFromStatus() error {
	pid := p.Pid
	statPath := common.HostProc(strconv.Itoa(int(pid)), "status")
	contents, err := ioutil.ReadFile(statPath)
	if err != nil {
		return err
	}
	lines := strings.Split(string(contents), "\n")
	for _, line := range lines {
		tabParts := strings.SplitN(line, "\t", 2)
		if len(tabParts) < 2 {
			continue
		}
		value := tabParts[1]
		switch strings.TrimRight(tabParts[0], ":") {
		case "Uid":
			p.uids = make([]int32, 0, 4)
			for _, i := range strings.Split(value, "\t") {
				v, err := strconv.ParseInt(i, 10, 32)
				if err != nil {
					return err
				}
				p.uids = append(p.uids, int32(v))
			}
		}
	}
	return nil
}

func getProcInodesAll(root string, max int) (map[string][]inodeMap, error) {
	pids, err := Pids()
	if err != nil {
		return nil, err
	}
	ret := make(map[string][]inodeMap)

	for _, pid := range pids {
		t, err := getProcInodes(root, pid, max)
		if err != nil {
			// skip if permission error or no longer exists
			if os.IsPermission(err) || os.IsNotExist(err) {
				continue
			}
			return ret, err
		}
		if len(t) == 0 {
			continue
		}
		// TODO: update ret.
		ret = updateMap(ret, t)
	}
	return ret, nil
}

// decodeAddress decode addresse represents addr in proc/net/*
// ex:
// "0500000A:0016" -> "10.0.0.5", 22
// "0085002452100113070057A13F025401:0035" -> "2400:8500:1301:1052:a157:7:154:23f", 53
func decodeAddress(family uint32, src string) (Addr, error) {
	t := strings.Split(src, ":")
	if len(t) != 2 {
		return Addr{}, fmt.Errorf("does not contain port, %s", src)
	}
	addr := t[0]
	port, err := strconv.ParseInt("0x"+t[1], 0, 64)
	if err != nil {
		return Addr{}, fmt.Errorf("invalid port, %s", src)
	}
	decoded, err := hex.DecodeString(addr)
	if err != nil {
		return Addr{}, fmt.Errorf("decode error, %s", err)
	}
	var ip net.IP
	// Assumes this is little_endian
	if family == syscall.AF_INET {
		ip = net.IP(Reverse(decoded))
	} else { // IPv6
		ip, err = parseIPv6HexString(decoded)
		if err != nil {
			return Addr{}, err
		}
	}
	return Addr{
		IP:   ip.String(),
		Port: uint32(port),
	}, nil
}

// Reverse reverses array of bytes.
func Reverse(s []byte) []byte {
	return ReverseWithContext(context.Background(), s)
}

func ReverseWithContext(ctx context.Context, s []byte) []byte {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
	return s
}

// parseIPv6HexString parse array of bytes to IPv6 string
func parseIPv6HexString(src []byte) (net.IP, error) {
	if len(src) != 16 {
		return nil, fmt.Errorf("invalid IPv6 string")
	}

	buf := make([]byte, 0, 16)
	for i := 0; i < len(src); i += 4 {
		r := Reverse(src[i : i+4])
		buf = append(buf, r...)
	}
	return net.IP(buf), nil
}

func processInet(file string, kind netConnectionKindType, inodes map[string][]inodeMap, filterPid int32) ([]connTmp, error) {

	if strings.HasSuffix(file, "6") && !common.PathExists(file) {
		// IPv6 not supported, return empty.
		return []connTmp{}, nil
	}

	// Read the contents of the /proc file with a single read sys call.
	// This minimizes duplicates in the returned connections
	// For more info:
	// https://github.com/shirou/gopsutil/pull/361
	contents, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}

	lines := bytes.Split(contents, []byte("\n"))

	var ret []connTmp
	// skip first line
	for _, line := range lines[1:] {
		l := strings.Fields(string(line))
		if len(l) < 10 {
			continue
		}
		laddr := l[1]
		raddr := l[2]
		status := l[3]
		inode := l[9]
		pid := int32(0)
		fd := uint32(0)
		i, exists := inodes[inode]
		if exists {
			pid = i[0].pid
			fd = i[0].fd
		}
		if filterPid > 0 && filterPid != pid {
			continue
		}
		if kind.sockType == syscall.SOCK_STREAM {
			status = TCPStatuses[status]
		} else {
			status = "NONE"
		}
		la, err := decodeAddress(kind.family, laddr)
		if err != nil {
			continue
		}
		ra, err := decodeAddress(kind.family, raddr)
		if err != nil {
			continue
		}

		ret = append(ret, connTmp{
			fd:       fd,
			family:   kind.family,
			sockType: kind.sockType,
			laddr:    la,
			raddr:    ra,
			status:   status,
			pid:      pid,
		})
	}

	return ret, nil
}

func processUnix(file string, kind netConnectionKindType, inodes map[string][]inodeMap, filterPid int32) ([]connTmp, error) {
	// Read the contents of the /proc file with a single read sys call.
	// This minimizes duplicates in the returned connections
	// For more info:
	// https://github.com/shirou/gopsutil/pull/361
	contents, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}

	lines := bytes.Split(contents, []byte("\n"))

	var ret []connTmp
	// skip first line
	for _, line := range lines[1:] {
		tokens := strings.Fields(string(line))
		if len(tokens) < 6 {
			continue
		}
		st, err := strconv.Atoi(tokens[4])
		if err != nil {
			return nil, err
		}

		inode := tokens[6]

		var pairs []inodeMap
		pairs, exists := inodes[inode]
		if !exists {
			pairs = []inodeMap{
				{},
			}
		}
		for _, pair := range pairs {
			if filterPid > 0 && filterPid != pair.pid {
				continue
			}
			var path string
			if len(tokens) == 8 {
				path = tokens[len(tokens)-1]
			}
			ret = append(ret, connTmp{
				fd:       pair.fd,
				family:   kind.family,
				sockType: uint32(st),
				laddr: Addr{
					IP: path,
				},
				pid:    pair.pid,
				status: "NONE",
				path:   path,
			})
		}
	}

	return ret, nil
}

func updateMap(src map[string][]inodeMap, add map[string][]inodeMap) map[string][]inodeMap {
	for key, value := range add {
		a, exists := src[key]
		if !exists {
			src[key] = value
			continue
		}
		src[key] = append(a, value...)
	}
	return src
}
