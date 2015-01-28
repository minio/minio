package tdlog

import (
	"fmt"
	"log"
	"sync"
)

// Controls whether INFO log messages are generated
var VerboseLog = false

// LVL 6
func Infof(template string, params ...interface{}) {
	if VerboseLog {
		log.Printf(template, params...)
	}
}

func Info(params ...interface{}) {
	if VerboseLog {
		log.Print(params...)
	}
}

// LVL 5
func Noticef(template string, params ...interface{}) {
	log.Printf(template, params...)
}

func Notice(params ...interface{}) {
	log.Print(params...)
}

var critHistory = make(map[string]struct{})
var critLock = new(sync.Mutex)

// LVL 2 - will not repeat a message twice over the past 100 distinct crit messages
func CritNoRepeat(template string, params ...interface{}) {
	msg := fmt.Sprintf(template, params...)
	critLock.Lock()
	if _, exists := critHistory[msg]; !exists {
		log.Print(msg)
		critHistory[msg] = struct{}{}
	}
	if len(critHistory) > 100 {
		critHistory = make(map[string]struct{})
	}
	critLock.Unlock()
}

// LVL 1
func Panicf(template string, params ...interface{}) {
	log.Panicf(template, params...)
}
