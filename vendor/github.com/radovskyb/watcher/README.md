# watcher

[![Build Status](https://travis-ci.org/radovskyb/watcher.svg?branch=master)](https://travis-ci.org/radovskyb/watcher)

`watcher` is a Go package for watching for files or directory changes (recursively or non recursively) without using filesystem events, which allows it to work cross platform consistently.

`watcher` watches for changes and notifies over channels either anytime an event or an error has occurred.

Events contain the `os.FileInfo` of the file or directory that the event is based on and the type of event and file or directory path.

[Installation](#installation)  
[Features](#features)  
[Example](#example)  
[Contributing](#contributing)  
[Watcher Command](#command)  

# Update
Event.Path for Rename and Move events is now returned in the format of `fromPath -> toPath`

#### Chmod event is not supported under windows.

# Installation

```shell
go get -u github.com/radovskyb/watcher/...
```

# Features

- Customizable polling interval.
- Filter Events.
- Watch folders recursively or non-recursively.
- Choose to ignore hidden files.
- Choose to ignore specified files and folders.
- Notifies the `os.FileInfo` of the file that the event is based on. e.g `Name`, `ModTime`, `IsDir`, etc.
- Notifies the full path of the file that the event is based on or the old and new paths if the event was a `Rename` or `Move` event.
- Limit amount of events that can be received per watching cycle.
- List the files being watched.
- Trigger custom events.

# Todo

- Write more tests.
- Write benchmarks.

# Example

```go
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/radovskyb/watcher"
)

func main() {
	w := watcher.New()

	// SetMaxEvents to 1 to allow at most 1 event's to be received
	// on the Event channel per watching cycle.
	//
	// If SetMaxEvents is not set, the default is to send all events.
	w.SetMaxEvents(1)

	// Only notify rename and move events.
	w.FilterOps(watcher.Rename, watcher.Move)

	go func() {
		for {
			select {
			case event := <-w.Event:	
				fmt.Println(event) // Print the event's info.
			case err := <-w.Error:
				log.Fatalln(err)
			case <-w.Closed:
				return
			}
		}
	}()

	// Watch this folder for changes.
	if err := w.Add("."); err != nil {
		log.Fatalln(err)
	}

	// Watch test_folder recursively for changes.
	if err := w.AddRecursive("../test_folder"); err != nil {
		log.Fatalln(err)
	}

	// Print a list of all of the files and folders currently
	// being watched and their paths.
	for path, f := range w.WatchedFiles() {
		fmt.Printf("%s: %s\n", path, f.Name())
	}

	fmt.Println()

	// Trigger 2 events after watcher started.
	go func() {
		w.Wait()
		w.TriggerEvent(watcher.Create, nil)
		w.TriggerEvent(watcher.Remove, nil)
	}()

	// Start the watching process - it'll check for changes every 100ms.
	if err := w.Start(time.Millisecond * 100); err != nil {
		log.Fatalln(err)
	}
}
```

# Contributing
If you would ike to contribute, simply submit a pull request.

# Command

`watcher` comes with a simple command which is installed when using the `go get` command from above.

# Usage

```
Usage of watcher:
  -cmd string
    	command to run when an event occurs
  -dotfiles
    	watch dot files (default true)
  -interval string
    	watcher poll interval (default "100ms")
  -keepalive
    	keep alive when a cmd returns code != 0
  -list
    	list watched files on start
  -pipe
    	pipe event's info to command's stdin
  -recursive
    	watch folders recursively (default true)
  -startcmd
    	run the command when watcher starts
```

All of the flags are optional and watcher can also be called by itself:
```shell
watcher
```
(watches the current directory recursively for changes and notifies any events that occur.)

A more elaborate example using the `watcher` command:
```shell
watcher -dotfiles=false -recursive=false -cmd="./myscript" main.go ../
```
In this example, `watcher` will ignore dot files and folders and won't watch any of the specified folders recursively. It will also run the script `./myscript` anytime an event occurs while watching `main.go` or any files or folders in the previous directory (`../`).

Using the `pipe` and `cmd` flags together will send the event's info to the command's stdin when changes are detected.

First create a file called `script.py` with the following contents:
```python
import sys

for line in sys.stdin:
	print (line + " - python")
```

Next, start watcher with the `pipe` and `cmd` flags enabled:
```shell
watcher -cmd="python script.py" -pipe=true
```

Now when changes are detected, the event's info will be output from the running python script.
