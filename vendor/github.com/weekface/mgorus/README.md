# Mongodb Hooks for [Logrus](https://github.com/Sirupsen/logrus) <img src="http://i.imgur.com/hTeVwmJ.png" width="40" height="40" alt=":walrus:" class="emoji" title=":walrus:"/>

## Install

```shell
$ go get github.com/weekface/mgorus
```

## Usage

```go
package main

import (
	"github.com/Sirupsen/logrus"
	"github.com/weekface/mgorus"
)

func main() {
	log := logrus.New()
	hooker, err := mgorus.NewHooker("localhost:27017", "db", "collection")
	if err == nil {
	    log.Hooks.Add(hooker)
	}

	log.WithFields(logrus.Fields{
		"name": "zhangsan",
		"age":  28,
	}).Error("Hello world!")
}
```

## License
*MIT*
