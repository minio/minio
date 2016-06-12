// +build linux darwin freebsd netbsd openbsd solaris dragonfly
// +build !appengine

package pb

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"unsafe"
)

const (
	TIOCGWINSZ     = 0x5413
	TIOCGWINSZ_OSX = 1074295912
)

var tty *os.File

var ErrPoolWasStarted = errors.New("Bar pool was started")

var echoLocked bool
var echoLockMutex sync.Mutex

func init() {
	var err error
	tty, err = os.Open("/dev/tty")
	if err != nil {
		tty = os.Stdin
	}
}

// terminalWidth returns width of the terminal.
func terminalWidth() (int, error) {
	w := new(window)
	tio := syscall.TIOCGWINSZ
	if runtime.GOOS == "darwin" {
		tio = TIOCGWINSZ_OSX
	}
	res, _, err := syscall.Syscall(sysIoctl,
		tty.Fd(),
		uintptr(tio),
		uintptr(unsafe.Pointer(w)),
	)
	if int(res) == -1 {
		return 0, err
	}
	return int(w.Col), nil
}

var oldState syscall.Termios

func lockEcho() (quit chan int, err error) {
	echoLockMutex.Lock()
	defer echoLockMutex.Unlock()
	if echoLocked {
		err = ErrPoolWasStarted
		return
	}
	echoLocked = true

	fd := tty.Fd()
	if _, _, e := syscall.Syscall6(sysIoctl, fd, ioctlReadTermios, uintptr(unsafe.Pointer(&oldState)), 0, 0, 0); e != 0 {
		err = fmt.Errorf("Can't get terminal settings: %v", e)
		return
	}

	newState := oldState
	newState.Lflag &^= syscall.ECHO
	newState.Lflag |= syscall.ICANON | syscall.ISIG
	newState.Iflag |= syscall.ICRNL
	if _, _, e := syscall.Syscall6(sysIoctl, fd, ioctlWriteTermios, uintptr(unsafe.Pointer(&newState)), 0, 0, 0); e != 0 {
		err = fmt.Errorf("Can't set terminal settings: %v", e)
		return
	}
	quit = make(chan int, 1)
	go catchTerminate(quit)
	return
}

func unlockEcho() (err error) {
	echoLockMutex.Lock()
	defer echoLockMutex.Unlock()
	if !echoLocked {
		return
	}
	echoLocked = false
	fd := tty.Fd()
	if _, _, e := syscall.Syscall6(sysIoctl, fd, ioctlWriteTermios, uintptr(unsafe.Pointer(&oldState)), 0, 0, 0); e != 0 {
		err = fmt.Errorf("Can't set terminal settings")
	}
	return
}

// listen exit signals and restore terminal state
func catchTerminate(quit chan int) {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGKILL)
	defer signal.Stop(sig)
	select {
	case <-quit:
		unlockEcho()
	case <-sig:
		unlockEcho()
	}
}
