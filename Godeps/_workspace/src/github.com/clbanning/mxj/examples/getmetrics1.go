// getmetrics1.go - transform Eclipse Metrics (v3) XML report into CSV files for each metric
// Uses NewMapXmlReader on os.File without copying the raw XML into a buffer while decoding.. 
// Shows no significant overhead for not first buffering large XML file as in getmetrics2.go.

/*
I needed to convert a large (14.9 MB) XML data set from an Eclipse metrics report on an
application that had 355,100 lines of code in 211 packages into CSV data sets.  The report
included application-, package-, class- and method-level metrics reported in an element,
"Value", with varying attributes.
	<Value value=""/>
	<Value name="" package="" value=""/>
	<Value name="" source="" package="" value=""/>
	<Value name="" source="" package="" value="" inrange=""/>

In addition, the metrics were reported with two different "Metric" compound elements:
	<Metrics>
		<Metric id="" description="">
			<Values>
				<Value.../>
				...
			</Values>
		</Metric>
		...
		<Metric id="" description="">
			<Value.../>
		</Metric>
		...
	</Metrics>

To run this example, extract the metrics_data.xml file from metrics_data.zip, then:
	> go run getmetrics1 -file=metrics_data.xml

The output will be a set of "csv" files.
*/

package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"
	"github.com/clbanning/mxj"
)

func main() {
	var file string
	flag.StringVar(&file, "file", "", "file to process")
	flag.Parse()

	fh, fherr := os.Open(file)
	if fherr != nil {
		fmt.Println("fherr:", fherr.Error())
		return
	}
	defer fh.Close()
	fmt.Println(time.Now().String(), "... File Opened:", file)

	/*
		// Get the XML data set from the file.
		fs, _ := fh.Stat()
		xmldata := make([]byte, fs.Size())
		n, frerr := fh.Read(xmldata)
		if frerr != nil {
			fmt.Println("frerr:", frerr.Error())
			return
		}
		if int64(n) != fs.Size() {
			fmt.Println("n:", n, "fs.Size():", fs.Size())
			return
		}
		fmt.Println(time.Now().String(), "... File Read - size:", fs.Size())

		// load XML into a Map value
		m, merr := mxj.NewMapXml(xmldata)
	*/
	// Consume the file using os.File Reader.
	// Note: there is a single record with root tag of "Metrics".
	m, merr := mxj.NewMapXmlReader(fh)
	if merr != nil {
		log.Fatal("merr:", merr.Error())
	}
	fmt.Println(time.Now().String(), "... XML Unmarshaled - len:", len(m))

	// Get just the key values of interest.
	// Could also use m.ValuesForKey("Metric"),
	// since there's just the one path.
	metricVals, err := m.ValuesForPath("Metrics.Metric")
	if err != nil {
		log.Fatal("err:", err.Error())
	}
	fmt.Println(time.Now().String(), "... ValuesFromKeyPath - len:", len(metricVals))

	// now just manipulate Map entries returned as []interface{} array.
	for _, v := range metricVals {
		aMetricVal := v.(map[string]interface{})

		// create file to hold csv data sets
		id := aMetricVal["-id"].(string)
		desc := aMetricVal["-description"].(string)
		mf, mferr := os.OpenFile(id+".csv", os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0666)
		if mferr != nil {
			fmt.Println("mferr:", mferr.Error())
			return
		}

		fmt.Print(time.Now().String(), " id: ", id, " desc: ", desc)
		mf.WriteString(id + "," + desc + "\n")

		// rescan looking for keys with data: Values or Value
		for key, val := range aMetricVal {
			switch key {
			case "Values":
				// extract the list of "Value" from map
				values := val.(map[string]interface{})["Value"].([]interface{})
				fmt.Println(" len(Values):", len(values))

				// first line in file is the metric label values (keys)
				var gotKeys bool
				for _, vval := range values {
					valueEntry := vval.(map[string]interface{})

					// no guarantee that range on map will follow any sequence
					lv := len(valueEntry)
					type ev [2]string
					list := make([]ev, lv)
					var i int
					for k, v := range valueEntry {
						list[i][0] = k
						list[i][1] = v.(string)
						i++
					}

					// extract keys as column header on first pass
					if !gotKeys {
						// print out the keys
						var gotFirstKey bool
						// for kk, _ := range valueEntry {
						for i := 0; i < lv; i++ {
							if gotFirstKey {
								mf.WriteString(",")
							} else {
								gotFirstKey = true
							}
							// strip prepended hyphen
							mf.WriteString((list[i][0])[1:])
						}
						mf.WriteString("\n")
						gotKeys = true
					}

					// print out values
					var gotFirstVal bool
					// for _, vv := range valueEntry {
					for i := 0; i < lv; i++ {
						if gotFirstVal {
							mf.WriteString(",")
						} else {
							gotFirstVal = true
						}
						mf.WriteString(list[i][1])
					}

					// terminate row of data
					mf.WriteString("\n")
				}
			case "Value":
				vv := val.(map[string]interface{})
				fmt.Println(" len(Value):", len(vv))
				mf.WriteString("value\n" + vv["-value"].(string) + "\n")
			}
		}
		mf.Close()
	}
}
