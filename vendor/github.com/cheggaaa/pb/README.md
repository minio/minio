# Terminal progress bar for Go  

Simple progress bar for console programs. 
    

## Installation

```
go get gopkg.in/cheggaaa/pb.v1
```   

## Usage   

```Go
package main

import (
	"gopkg.in/cheggaaa/pb.v1"
	"time"
)

func main() {
	count := 100000
	bar := pb.StartNew(count)
	for i := 0; i < count; i++ {
		bar.Increment()
		time.Sleep(time.Millisecond)
	}
	bar.FinishPrint("The End!")
}

```

Result will be like this:

```
> go run test.go
37158 / 100000 [================>_______________________________] 37.16% 1m11s
```

## Customization

```Go  
// create bar
bar := pb.New(count)

// refresh info every second (default 200ms)
bar.SetRefreshRate(time.Second)

// show percents (by default already true)
bar.ShowPercent = true

// show bar (by default already true)
bar.ShowBar = true

// no counters
bar.ShowCounters = false

// show "time left"
bar.ShowTimeLeft = true

// show average speed
bar.ShowSpeed = true

// sets the width of the progress bar
bar.SetWidth(80)

// sets the width of the progress bar, but if terminal size smaller will be ignored
bar.SetMaxWidth(80)

// convert output to readable format (like KB, MB)
bar.SetUnits(pb.U_BYTES)

// and start
bar.Start()
``` 

## Progress bar for IO Operations

```go
// create and start bar
bar := pb.New(myDataLen).SetUnits(pb.U_BYTES)
bar.Start()

// my io.Reader
r := myReader

// my io.Writer
w := myWriter

// create proxy reader
reader := bar.NewProxyReader(r)

// and copy from pb reader
io.Copy(w, reader)

```

```go
// create and start bar
bar := pb.New(myDataLen).SetUnits(pb.U_BYTES)
bar.Start()

// my io.Reader
r := myReader

// my io.Writer
w := myWriter

// create multi writer
writer := io.MultiWriter(w, bar)

// and copy
io.Copy(writer, r)

bar.Finish()
```

## Custom Progress Bar Look-and-feel

```go
bar.Format("<.- >")
```

## Multiple Progress Bars (experimental and unstable)

Do not print to terminal while pool is active.

```go
package main

import (
    "math/rand"
    "sync"
    "time"

    "gopkg.in/cheggaaa/pb.v1"
)

func main() {
    // create bars
    first := pb.New(200).Prefix("First ")
    second := pb.New(200).Prefix("Second ")
    third := pb.New(200).Prefix("Third ")
    // start pool
    pool, err := pb.StartPool(first, second, third)
    if err != nil {
        panic(err)
    }
    // update bars
    wg := new(sync.WaitGroup)
    for _, bar := range []*pb.ProgressBar{first, second, third} {
        wg.Add(1)
        go func(cb *pb.ProgressBar) {
            for n := 0; n < 200; n++ {
                cb.Increment()
                time.Sleep(time.Millisecond * time.Duration(rand.Intn(100)))
            }
            cb.Finish()
            wg.Done()
        }(bar)
    }
    wg.Wait()
    // close pool
    pool.Stop()
}
```

The result will be as follows:

```
$ go run example/multiple.go 
First 141 / 1000 [===============>---------------------------------------] 14.10 % 44s
Second 139 / 1000 [==============>---------------------------------------] 13.90 % 44s
Third 152 / 1000 [================>--------------------------------------] 15.20 % 40s
```
