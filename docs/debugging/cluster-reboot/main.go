package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime/debug"
	"slices"
	"sort"
	"strconv"
	"time"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"golang.org/x/crypto/ssh"
)

type Pool struct {
	Servers map[string]*Server
	// Sets    map[int]*Set
}

type Server struct {
	Sets      map[int]*Set
	Endpoint  string
	Rebooted  bool
	Processed bool
}

type Set struct {
	SCParity   int
	RRSCParity int
	BadDisks   int
	ID         int
	Pool       int
	CanReboot  bool
	Disks      map[string]*Disk
}

type Disk struct {
	UUID           string
	Index          int
	Pool           int
	Server         string
	Set            int
	Path           string
	State          string
	UsedPercentage float64
}

var (
	endpoint    string
	miniokey    string
	miniosecret string
	secure      bool
	jsonOutput  bool

	badSetsOnly bool

	folder   string
	hostfile string
	port     string
)

var mclient *madmin.AdminClient

func jsonOut(b interface{}) {
	outb, err := json.Marshal(b)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(outb))
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("invalid number of arguments.. try --help")
		os.Exit(1)
	}

	switch parseArgs() {
	case "hostfile":
		makeHostfile()
	case "reboot":
		rebootHostfile()
	case "health":
		healthCheck()
	case "sets":
		sets()
	default:
		flag.Usage()
	}
}

func parseArgs() (command string) {
	hasHelp := false
	if slices.Contains(os.Args, "--help") {
		hasHelp = true
	}
	command = os.Args[1]
	os.Args = os.Args[1:]
	switch command {

	case "hostfile":
		flag.StringVar(&folder, "folder", "./cluster-hostfiles", "Hostfiles will be placed in this folder")
		flag.StringVar(&port, "port", "", "minio port")
		if hasHelp {
			flag.Parse()
			flag.Usage()
			os.Exit(1)
		}

	case "reboot":
		flag.StringVar(&hostfile, "hostfile", "", "The list of hosts to be rebooted")
		flag.StringVar(&port, "port", "", "minio port")
		if hasHelp {
			flag.Parse()
			flag.Usage()
			os.Exit(1)
		}
	case "health":
		flag.StringVar(&hostfile, "hostfile", "", "The list of hosts to be monitored for health")
		flag.StringVar(&port, "port", "", "minio port")
		if hasHelp {
			flag.Parse()
			flag.Usage()
			os.Exit(1)
		}
	case "sets":
		flag.StringVar(&port, "port", "", "ssh port")
		flag.BoolVar(&jsonOutput, "json", false, "Print output in json")
		flag.BoolVar(&badSetsOnly, "badSetsOnly", false, "Show only bad disks")
		if hasHelp {
			flag.Parse()
			flag.Usage()
			os.Exit(1)
		}
	default:
	}

	flag.StringVar(&endpoint, "endpoint", "127.0.0.1", "server endpoint")
	flag.StringVar(&miniokey, "key", "minioadmin", "minio user/key")
	flag.StringVar(&miniosecret, "secret", "minioadmin", "minio password/secret")
	flag.BoolVar(&secure, "secure", false, "Toggle SSL on/off")
	flag.Parse()
	if hasHelp {
		printCommands()
		flag.Usage()
		os.Exit(1)
	}
	return
}

func printCommands() {
	fmt.Println("")
	fmt.Println(" Available commands")
	fmt.Println(" -----------------------------")
	fmt.Println(" sets       Shows which servers are in which sets")
	fmt.Println(" hostfile   Generates hostfiles in `-folder`")
	fmt.Println(" reboot     Reboots servers defined in `-hostfile`")
	fmt.Println(" monitor    Monitors the uptime of hosts defined in `-hostfile`")
	fmt.Println(" -----------------------------")
	fmt.Println("")
}

func makeClient() (err error) {
	ep := endpoint + ":" + port
	mclient, err = madmin.NewWithOptions(ep, &madmin.Options{
		Creds:     credentials.NewStaticV4(miniokey, miniosecret, ""),
		Secure:    secure,
		Transport: DefaultTransport(secure),
	})
	return
}

func sets() {
	pools, _, err := getInfra()
	if err != nil {
		panic(err)
	}

	type settemp struct {
		Disks     []*Disk
		CanReboot bool
		Parity    int
		BadDisks  int
	}

	sets := make(map[string]map[int]*settemp)
	for pid, p := range pools {
		sets[pid] = make(map[int]*settemp, 0)
		for _, s := range p.Servers {
			for _, set := range s.Sets {
				_, ok := sets[pid][set.ID]
				if !ok {
					sets[pid][set.ID] = new(settemp)
				}

				sets[pid][set.ID].Parity = set.SCParity
				sets[pid][set.ID].CanReboot = set.CanReboot
				sets[pid][set.ID].BadDisks = set.BadDisks

				for _, d := range set.Disks {
					if badSetsOnly {
						if d.State != "ok" {
							sets[pid][set.ID].Disks = append(sets[pid][set.ID].Disks, d)
						}
					} else {
						sets[pid][set.ID].Disks = append(sets[pid][set.ID].Disks, d)
					}
				}
			}
		}
	}

	if jsonOutput {
		jsonOut(sets)
		return
	}

	for i, v := range sets {
		for ii, vv := range v {
			toPrint := []string{}
			for _, vvv := range vv.Disks {
				toPrint = append(toPrint, fmt.Sprint(vvv.State, " ", vvv.Server))
			}
			if len(toPrint) < 1 {
				continue
			}

			fmt.Printf("\nPool(%s) SET(%d) CanReboot(%t) Parity(%d) BadDisks(%d)\n", i, ii, vv.CanReboot, vv.Parity, vv.BadDisks)
			for _, p := range toPrint {
				fmt.Println(p)
			}
		}
	}
}

func getInfra() (pools map[string]*Pool, totalServers int, err error) {
	err = makeClient()
	if err != nil {
		panic(err)
	}

	var info madmin.StorageInfo
	info, err = mclient.StorageInfo(context.Background())
	if err != nil {
		return
	}
	// bb, err := os.ReadFile("storage_info.json")
	// if err != nil {
	// 	panic(err)
	// }
	// err = json.Unmarshal(bb, &info)
	// if err != nil {
	// 	panic(err)
	// }

	setInfo := make(map[string]map[string]*Set)

	pools = make(map[string]*Pool, 0)
	for _, d := range info.Disks {
		if setInfo[strconv.Itoa(d.PoolIndex)] == nil {
			setInfo[strconv.Itoa(d.PoolIndex)] = make(map[string]*Set, 0)
		}

		pool, ok := pools[strconv.Itoa(d.PoolIndex)]
		if !ok {
			pools[strconv.Itoa(d.PoolIndex)] = &Pool{
				Servers: make(map[string]*Server, 0),
			}
			pool = pools[strconv.Itoa(d.PoolIndex)]
		}

		x, errx := url.Parse(d.Endpoint)
		if errx != nil || x == nil {
			panic(errx)
		}

		server, ok := pool.Servers[x.Hostname()]
		if !ok {
			pool.Servers[x.Hostname()] = &Server{
				Sets:     make(map[int]*Set, 0),
				Rebooted: false,
				Endpoint: x.Hostname(),
			}
			server = pool.Servers[x.Hostname()]
			totalServers++
		}

		set, ok := server.Sets[d.SetIndex]
		if !ok {
			server.Sets[d.SetIndex] = &Set{
				Disks:      make(map[string]*Disk, 0),
				SCParity:   info.Backend.StandardSCParity,
				RRSCParity: info.Backend.RRSCParity,
				ID:         d.SetIndex,
				Pool:       d.PoolIndex,
				CanReboot:  false,
			}
			set = server.Sets[d.SetIndex]
		}

		seti, ok := setInfo[strconv.Itoa(d.PoolIndex)][strconv.Itoa(d.SetIndex)]
		if !ok {
			setInfo[strconv.Itoa(d.PoolIndex)][strconv.Itoa(d.SetIndex)] = &Set{
				SCParity:   info.Backend.StandardSCParity,
				RRSCParity: info.Backend.RRSCParity,
				ID:         d.SetIndex,
				Pool:       d.PoolIndex,
				BadDisks:   0,
				CanReboot:  true,
			}
			seti = setInfo[strconv.Itoa(d.PoolIndex)][strconv.Itoa(d.SetIndex)]
		}

		if d.State != "ok" {
			seti.BadDisks++
		}

		set.Disks[d.Endpoint] = &Disk{
			UUID:           d.UUID,
			Index:          d.DiskIndex,
			Pool:           d.PoolIndex,
			Server:         d.Endpoint,
			Set:            d.SetIndex,
			Path:           d.DrivePath,
			State:          d.State,
			UsedPercentage: (math.Ceil((float64(d.UsedSpace)/float64(d.TotalSpace)*100)*100) / 100),
		}
	}

	for i, v := range pools {
		for _, vv := range v.Servers {
			for iii, vvv := range vv.Sets {
				seti, ok := setInfo[i][strconv.Itoa(iii)]
				if ok {
					if seti.BadDisks >= (seti.SCParity - 1) {
						vvv.CanReboot = false
					} else {
						vvv.CanReboot = true
					}
					vvv.BadDisks = seti.BadDisks
				}
			}
		}
	}

	return
}

// stringKeysSorted returns the keys as a sorted string slice.
func stringKeysSorted[K string, V any](m map[K]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, string(k))
	}
	sort.Strings(keys)
	return keys
}

func makeHostfile() {
	pools, totalServers, err := getInfra()
	var rebootRounds [200][200]map[string]*Server
	processed := 0
	poolss := stringKeysSorted(pools)
	for i := 0; i < len(rebootRounds); i++ {
		if processed >= totalServers {
			fmt.Printf("Total (%d) Online (%d)\n", totalServers, processed)
			break
		}

		for _, pkey := range poolss {
			pid, err := strconv.Atoi(pkey)
			if err != nil {
				panic(err)
			}
			v := pools[pkey]
			if rebootRounds[i][pid] == nil {
				rebootRounds[i][pid] = make(map[string]*Server)
			}

			sortServKey := stringKeysSorted(v.Servers)
		nextServer:
			for _, skey := range sortServKey {
				s := v.Servers[skey]
				if s.Processed {
					continue
				}

				if !areAllSetsOK(s) {
					fmt.Println("can't reboot:", s.Endpoint)
					continue
				}

				_, ok := rebootRounds[i][pid][s.Endpoint]
				if !ok {

					for _, rv := range rebootRounds[i][pid] {
						if haveMatchingSets(rv, s) {
							continue nextServer
						}
					}

					rebootRounds[i][pid][s.Endpoint] = pools[pkey].Servers[skey]
					pools[pkey].Servers[skey].Processed = true
					processed++
				} else {
					continue
				}

			}
		}
	}

	_ = os.RemoveAll(folder)
	err = os.MkdirAll(folder, 0o777)
	if err != nil {
		panic(err)
	}

	var roundFile *os.File

	for ri, rv := range rebootRounds {
		for _, rv2 := range rv {
			if rv2 != nil && len(rv2) > 0 {
				roundFile, err = os.OpenFile(filepath.Join(folder, "round-"+strconv.Itoa(ri)), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o777)
				if err != nil {
					panic(err)
				}
				srvSort := stringKeysSorted(rv2)
				for _, rvkey := range srvSort {
					_, err = roundFile.WriteString(rv2[rvkey].Endpoint + "\n")
					if err != nil {
						panic(err)
					}
				}
				roundFile.Sync()
				roundFile.Close()
			}
		}
	}
}

func areAllSetsOK(s1 *Server) (yes bool) {
	for _, set := range s1.Sets {
		if !set.CanReboot {
			return false
		}
	}

	return true
}

func haveMatchingSets(s1 *Server, s2 *Server) (yes bool) {
	for setid := range s1.Sets {
		_, ok := s2.Sets[setid]
		if ok {
			return true
		}
	}

	return false
}

var DefaultTransport = func(secure bool) http.RoundTripper {
	tr := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:       5 * time.Second,
			KeepAlive:     15 * time.Second,
			FallbackDelay: 100 * time.Millisecond,
		}).DialContext,
		MaxIdleConns:          1024,
		MaxIdleConnsPerHost:   1024,
		ResponseHeaderTimeout: 60 * time.Second,
		IdleConnTimeout:       60 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DisableCompression:    true,
	}

	if secure {
		tr.TLSClientConfig = &tls.Config{
			InsecureSkipVerify: true,
			MinVersion:         tls.VersionTLS12,
		}
	}
	return tr
}

func healthCheck() {
	defer func() {
		r := recover()
		if r != nil {
			log.Println(r, string(debug.Stack()))
		}
	}()

	hosts, err := os.ReadFile(hostfile)
	if err != nil {
		panic(err)
	}
	hostsList := bytes.Split(hosts, []byte{10})

	hostMap := make(map[string]bool)
	for _, v := range hostsList {
		if len(v) < 1 {
			continue
		}
		hostMap[string(v)] = false
	}

	defer func() {
		fmt.Println("Post run host report...")
		for i, v := range hostMap {
			if v {
				fmt.Println("healthy:", i)
			} else {
				fmt.Println("unhealthy:", i)
			}
		}
	}()

	unhealthy := 0
	for {
		unhealthy = 0
		for host, healthy := range hostMap {
			if healthy {
				continue
			}
			ok, err := healthPing(host)
			if err != nil {
				unhealthy++
				fmt.Println(err)
				hostMap[host] = false
			} else if !ok {
				unhealthy++
				fmt.Println("Waiting:", host)
				hostMap[host] = false
			} else {
				hostMap[host] = true
			}
		}
		if unhealthy == 0 {
			return
		}
		fmt.Println("unhealthy hosts: ", unhealthy)
		time.Sleep(30 * time.Second)
	}
}

func rebootHostfile() {
	defer func() {
		r := recover()
		if r != nil {
			log.Println(r, string(debug.Stack()))
		}
	}()

	hosts, err := os.ReadFile(hostfile)
	if err != nil {
		panic(err)
	}
	hostsList := bytes.Split(hosts, []byte{10})
	for _, v := range hostsList {
		if len(v) < 1 {
			continue
		}
		rebootServer(string(v))
	}
}

func rebootServer(host string) {
	config := &ssh.ClientConfig{
		User:            "root",
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         10 * time.Second,
	}
	fmt.Println("Rebooting:", host)
	con, err := ssh.Dial("tcp", host+":"+port, config)
	if err != nil {
		fmt.Println(err)
		return
	}
	session, err := con.NewSession()
	if err != nil {
		fmt.Println(err)
		return
	}
	defer session.Close()

	output, err := session.CombinedOutput("echo 'this is where we execute the command'")
	if err != nil {
		fmt.Printf("Command failed @ %s .. err: %v\n", host, err)
		fmt.Printf("Output: %s\n", output)
		return
	}

	fmt.Println("Rebooted:", host, "output:", string(output))
}

func healthPing(endpoint string) (healthy bool, err error) {
	client := new(http.Client)
	client.Transport = DefaultTransport(secure)
	url := "http://" + endpoint + ":" + port + "/minio/health/cluster"
	if secure {
		url = "https://" + endpoint + ":" + port + "/minio/health/cluster"
	}
	resp, rerr := client.Get(url)
	if rerr != nil {
		err = rerr
		return
	}

	if resp.StatusCode != 200 {
		return false, nil
	}

	return true, nil
}
