package main

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
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

	folder   string
	hostfile string
)

var mclient *madmin.AdminClient

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
		command = "hostfile"
		flag.StringVar(&folder, "folder", "./cluster-hostfiles", "Hostfiles will be placed in this folder")
		if hasHelp {
			flag.Parse()
			flag.Usage()
			os.Exit(1)
		}

	case "reboot":
		flag.StringVar(&hostfile, "hostfile", "", "The list of hosts to be rebooted")
		if hasHelp {
			flag.Parse()
			flag.Usage()
			os.Exit(1)
		}
	case "health":
		flag.StringVar(&hostfile, "hostfile", "", "The list of hosts to be monitored for health")
		if hasHelp {
			flag.Parse()
			flag.Usage()
			os.Exit(1)
		}
	default:
	}

	flag.StringVar(&endpoint, "endpoint", "127.0.0.1:9000", "server endpoint")
	flag.StringVar(&miniokey, "key", "minioadmin", "minio user/key")
	flag.StringVar(&miniosecret, "secret", "minioadmin", "minio password/secret")
	flag.BoolVar(&secure, "secure", false, "Toggle SSL on/off")
	flag.BoolVar(&jsonOutput, "json", false, "Print output in json")
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
	fmt.Println(" hostfile   Generates hostfiles in `-folder`")
	fmt.Println(" reboot     Reboots servers defined in `-hostfile`")
	fmt.Println(" monitor    Monitors the uptime of hosts defined in `-hostfile`")
	fmt.Println(" -----------------------------")
	fmt.Println("")
}

func makeClient() (err error) {
	mclient, err = madmin.NewWithOptions(endpoint, &madmin.Options{
		Creds:     credentials.NewStaticV4(miniokey, miniosecret, ""),
		Secure:    secure,
		Transport: DefaultTransport(secure),
	})
	return
}

func makeHostfile() {
	// makeClient()
	// info, err := mclient.ServerInfo(context.Background())
	// if err != nil {
	// 	return
	// }
	bb, err := os.ReadFile("infra.json")
	if err != nil {
		panic(err)
	}

	var info madmin.InfoMessage
	err = json.Unmarshal(bb, &info)
	if err != nil {
		panic(err)
	}

	pools := make(map[int]*Pool)
	for i := range info.Pools {
		pools[i+1] = &Pool{
			Servers: make(map[string]*Server, 0),
		}
	}

	totalServers := 0
	for _, s := range info.Servers {
		if s.State == "offline" {
			continue
		}

		totalServers++
		pool := pools[s.PoolNumber]
		server, ok := pool.Servers[s.Endpoint]
		if !ok {
			pool.Servers[s.Endpoint] = &Server{
				Sets:     make(map[int]*Set, 0),
				Rebooted: false,
				Endpoint: s.Endpoint,
			}
			server = pool.Servers[s.Endpoint]
		}

		for _, d := range s.Disks {
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
	}

	var rebootRounds [200][200]map[string]*Server
	processed := 0
	for i := 0; i < len(rebootRounds); i++ {
		if processed >= totalServers {
			fmt.Printf("Total (%d) Online (%d)\n", totalServers, processed)
			break
		}

		for pid, v := range pools {
			if rebootRounds[i][pid] == nil {
				rebootRounds[i][pid] = make(map[string]*Server)
			}

		nextServer:
			for sid, s := range v.Servers {
				if s.Processed {
					continue
				}

				_, ok := rebootRounds[i][pid][s.Endpoint]
				if !ok {

					for _, rv := range rebootRounds[i][pid] {
						if haveMatchingSets(rv, s) {
							continue nextServer
						}
					}

					rebootRounds[i][pid][s.Endpoint] = pools[pid].Servers[sid]
					pools[pid].Servers[sid].Processed = true
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
				roundFile, err = os.Create(filepath.Join(folder, "round-"+strconv.Itoa(ri)))
				if err != nil {
					panic(err)
				}
				for _, rv3 := range rv2 {
					_, err = roundFile.WriteString(rv3.Endpoint + "\n")
					if err != nil {
						panic(err)
					}
				}
			}
		}
		roundFile.Sync()
		roundFile.Close()
	}
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
	host = strings.Replace(host, ":443", ":22", -1)
	fmt.Println("Rebooting:", host)
	con, err := ssh.Dial("tcp", host, config)
	if err != nil {
		panic(err)
	}
	session, err := con.NewSession()
	if err != nil {
		panic(err)
	}
	defer session.Close()

	output, err := session.CombinedOutput("echo 'this is where we execute the command'")
	if err != nil {
		fmt.Printf("Command failed @ %s .. err: %v\n", host, err)
		fmt.Printf("Output: %s\n", output)
		panic(err)
	}

	fmt.Println("Rebooted:", host)
	fmt.Printf("Output: %s\n", output)
}

func healthPing(endpoint string) (healthy bool, err error) {
	client := new(http.Client)
	client.Transport = DefaultTransport(secure)
	url := "http://" + endpoint + "/minio/health/cluster"
	if secure {
		url = "https://" + endpoint + "/minio/health/cluster"
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
