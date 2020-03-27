package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/minio/minio/pkg/s3select/json/jstream"
)

var (
	depthFlag   = flag.Int("d", 0, "emit values at depth <int>")
	kvFlag      = flag.Bool("kv", false, "output key value pairs. default behavior is to emit only JSON object values.")
	verboseFlag = flag.Bool("v", false, "output depth and offset details for each value")
	helpFlag    = flag.Bool("h", false, "display this help dialog")
)

func exitErr(err error) {
	fmt.Fprintf(os.Stderr, "[\033[31merror\033[0m] %s", err)
	os.Exit(1)
}

func printVal(mv *jstream.MetaValue) {
	b, err := json.Marshal(mv.Value)
	if err != nil {
		exitErr(err)
	}

	s := string(b)
	var label string

	switch mv.Value.(type) {
	case []interface{}:
		label = "array  "
	case float64:
		label = "float  "
	case jstream.KV:
		label = "kv     "
	case string:
		label = "string "
	case map[string]interface{}:
		label = "object "
	}

	if *verboseFlag {
		end := mv.Offset + mv.Length
		fmt.Printf("%d\t%03d\t%03d\t%s| %s\n", mv.Depth, mv.Offset, end, label, s)
		return
	}
	fmt.Printf("%s| %s\n", label, s)
}

func main() {
	flag.Parse()
	if *helpFlag {
		help()
		os.Exit(0)
	}

	if *verboseFlag {
		fmt.Println("depth\tstart\tend\ttype   | value")
	}

	decoder := jstream.NewDecoder(os.Stdin, *depthFlag)
	if *kvFlag {
		decoder = decoder.EmitKV()
	}
	metaValChan, err := decoder.Stream()
	if err != nil {
		exitErr(err)
	}
	for mv := range metaValChan {
		printVal(mv)
	}
	if err := decoder.Err(); err != nil {
		exitErr(err)
	}
}

var helpMsg = `jstream - stream parsed values from JSON

usage: jstream [options]

options:

  -d <n> emit values at depth n. if n < 0, all values will be emitted
  -v     output depth and offset details for each value
  -h     display this help dialog
`

func help() {
	fmt.Println(helpMsg)
}
