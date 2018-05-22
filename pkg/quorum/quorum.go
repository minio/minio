package quorum

import (
	"errors"
	"fmt"
	"reflect"
	"time"
)

// ErrTimeout - error to indicate functions call timed out.
var ErrTimeout = errors.New("timed out")

// ErrGaveUp - error to indicate functions call failed with too many errors.
var ErrGaveUp = errors.New("gave up by too many errors")

// Error - error type to hold function and it's error.
type Error struct {
	ID  string
	Err error
}

func (err Error) Error() string {
	return err.Error()
}

// Func - compatible function type for quorum call.  Func must run actual task in a goroutine and
// returns <-chan Error immediately.  The goroutine must close the channel on success; must send
// single Error on failure.  Below is an example Func
//
// var f = Func(func() <-chan Error {
// 	errch := make(chan Error)
// 	go func() {
// 		defer close(errch)
// 		n := rand.Int()
// 		if n%2 != 0 {
// 			time.Sleep(time.Duration(n) * time.Millisecond)
// 			return
// 		}
// 		errch <- Error{ID: strings.Itoa(n), Err: fmt.Errorf("%v is divisible by 2", n)}
// 	}()
// 	return errch
// })
//
type Func func() <-chan Error

// Call - calls functions (they are run in goroutines) and waits for at least quorum success till maxExecTime.
// If there are pending goroutines running after success quorum, it waits maxSuccessTime to make pending
// goroutines to complete to maximize more success replies.
//
// Finally it returns code and errors where
// code = 0 indicates all functions return success;
// code = 1 indicates more than quorum number of functions return success;
// code = -1 indicates quorum number of functions return failure or timed out
//
// errors slice have no order to indicate which Func failed.  Its up to Func to return respective relationship.
//
// Below is an example;
//
//	count := 4
//	quorum := 1 + count/2
//	functions := []Func{}
//	for i := 0; i < count; i++ {
//		functions = append(functions, f)
//	}
//	rc, errs := Exec(functions, quorum, 1*time.Second, 250*time.Millisecond)
//
func Call(functions []Func, quorum int, maxExecTime, maxSuccessWait time.Duration) (int, []error) {
	if functions == nil {
		panic("nil functions")
	}

	count := len(functions)
	if quorum < 1 || quorum > count {
		panic(fmt.Sprintf("quorum must be >= 1 and <= %v", count))
	}

	cases := []reflect.SelectCase{}
	for _, function := range functions {
		errch := function()
		cases = append(cases, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(errch),
		})
	}

	timer := time.NewTimer(maxExecTime)
	defer timer.Stop()
	cases = append(cases, reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(timer.C),
	})

	// counter contains how many goroutines completed successfully
	success := 0

	// flag to indicate whether timer is changed to success wait time.
	timerChanged := false

	// error slice containing errors returned from completed goroutines.
	errs := []error{}

	// error to hold timeout or gave up error.
	var err error

	for completed := 0; completed < count; completed++ {
		// Check whether success of all pending goroutines are going to make success quorum or not.
		// If not, no need to wait them to complete.
		if success+count-completed < quorum {
			err = ErrGaveUp
			break
		}

		index, value, received := reflect.Select(cases)

		// Last element of cases is always timer channel. As we received value from the timer,
		// no need to wait for replies from pending goroutines.
		if index+1 == len(cases) {
			if success < quorum {
				err = ErrTimeout
			}

			break
		}

		if !received {
			// As we got closed channel by the index i.e. no value received, it must be success.
			success++

			if success == count {
				break
			}

			if success >= quorum {
				// As we got success quorum, but add little wait to get more success reply.
				if !timerChanged {
					timerChanged = true
					timer.Stop()
					timer = time.NewTimer(maxSuccessWait)
					cases[len(cases)-1] = reflect.SelectCase{
						Dir:  reflect.SelectRecv,
						Chan: reflect.ValueOf(timer.C),
					}
				}
			}
		} else {
			// As we received a value from a channel, it must be an Error.
			e := value.Interface().(Error)
			errs = append(errs, &e)
		}

		// Delete element in cases by index.
		copy(cases[index:], cases[index+1:])
		cases[len(cases)-1] = reflect.SelectCase{}
		cases = cases[:len(cases)-1]
	}

	if err != nil {
		errs = append(errs, err)
	}

	rc := -1
	if success == count {
		rc = 0
	} else if success >= quorum {
		rc = 1
	}

	return rc, errs
}
