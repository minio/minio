package lkleader

import (
	"fmt"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"time"
)

// assert fails the test if the condition is false.
func assert(tb testing.TB, condition bool, msg string, v ...interface{}) {
	if !condition {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d: "+msg+"\033[39m\n\n", append([]interface{}{filepath.Base(file), line}, v...)...)
		tb.FailNow()
	}
}

// ok fails the test if an err is not nil.
func ok(tb testing.TB, err error) {
	if err != nil {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d: unexpected error: %s\033[39m\n\n", filepath.Base(file), line, err.Error())
		tb.FailNow()
	}
}

// equals fails the test if exp is not equal to act.
func equals(tb testing.TB, exp, act interface{}) {
	if !reflect.DeepEqual(exp, act) {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d:\n\n\texp: %#v\n\n\tgot: %#v\033[39m\n\n", filepath.Base(file), line, exp, act)
		tb.FailNow()
	}
}

func newLeader(t testing.TB) *Leader {
	lead, err := New(
		"localhost:9999",
		[]string{"localhost:9999", "localhost:9998"},
		DefaultOnLeader,
		DefaultOnFollower,
		10,
		100,
	)
	ok(t, err)
	return lead
}

func TestNew(t *testing.T) {
	type args struct {
		addr       string
		nodes      []string
		onLeader   func() error
		onFollower func() error
	}
	tests := []struct {
		name            string
		args            args
		want            *Leader
		wantErr         bool
		wantLeaderErr   bool
		wantFollowerErr bool
	}{
		{
			"00 init defaults",
			args{
				addr:  "localhost:9999",
				nodes: []string{"localhost:9999", "localhost:9998"},
			},
			&Leader{
				Addr:       "localhost:9999",
				Nodes:      []string{"localhost:9999", "localhost:9998"},
				OnLeader:   DefaultOnLeader,
				OnFollower: DefaultOnFollower,
			},
			false,
			false,
			false,
		},
		{
			"01 event methods",
			args{
				addr:  "localhost:9999",
				nodes: []string{"localhost:9999", "localhost:9998"},
			},
			&Leader{
				Addr:       "localhost:9999",
				Nodes:      []string{"localhost:9999", "localhost:9998"},
				OnLeader:   func() error { return fmt.Errorf("") },
				OnFollower: func() error { return fmt.Errorf("") },
			},
			false,
			false,
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := New(tt.args.addr, tt.args.nodes, tt.args.onLeader, tt.args.onFollower, 10, 10)
			if (err != nil) != tt.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			equals(t, got.Addr, tt.want.Addr)
			equals(t, got.Nodes, tt.want.Nodes)
			err = got.OnLeader()
			if (err != nil) != tt.wantLeaderErr {
				t.Errorf("OnLeader() error = %v, wantErr %v", err, tt.wantLeaderErr)
				return
			}
			err = got.OnFollower()
			if (err != nil) != tt.wantFollowerErr {
				t.Errorf("OnFollower() error = %v, wantErr %v", err, tt.wantFollowerErr)
				return
			}
		})
	}
}

func TestLeader_StartStop(t *testing.T) {
	lead := newLeader(t)
	go func() {
		err := lead.Start()
		if err != nil {
			t.Fatal(err)
		}
	}()
	time.Sleep(time.Millisecond * 10)
	lead.Stop()

	// Start with an invalid state
	lead.State.State(99)
	err := lead.Start()
	equals(t, "unknown state 99", err.Error())
}

func TestLeader_follower(t *testing.T) {
	// coverage only
	lead := newLeader(t)
	go lead.Start()
	go lead.follower()
	time.Sleep(time.Millisecond * 10)
	lead.Stop()

	// state change
	go lead.Start()
	go lead.follower()
	lead.State.State(StateCandidate)
	time.Sleep(time.Millisecond * 20)
	lead.Stop()

	// append entries
	lead.State.State(StateFollower)
	go lead.Start()
	go lead.follower()
	lead.State.HeartBeatEvent(&HeartBeatRequest{5, lead.State.ID()})
	time.Sleep(time.Millisecond * 20)
	lead.Stop()

	// heartbeat timeout
	lead.State.State(StateFollower)
	go lead.Start()
	go lead.leader()
	time.Sleep(time.Millisecond * 10)
	lead.Stop()

	wantErr := fmt.Errorf("test")
	lead.Lock()
	lead.OnFollower = func() error { return wantErr }
	lead.Unlock()
	err := lead.Start()
	equals(t, wantErr, err)
}

func TestLeader_candidate(t *testing.T) {
	// coverage only
	lead := newLeader(t)
	go lead.Start()
	go lead.candidate()
	time.Sleep(time.Millisecond * 10)
	lead.Stop()

	// state change
	go lead.Start()
	go lead.leader()
	lead.State.State(StateFollower)
	time.Sleep(time.Millisecond * 20)
	lead.Stop()

	// election timeout
	lead.State.State(StateCandidate)
	go lead.Start()
	go lead.leader()
	time.Sleep(time.Millisecond * 10)
	lead.Stop()
}

func TestLeader_leader(t *testing.T) {
	// coverage only
	lead := newLeader(t)
	go lead.Start()
	go lead.leader()
	time.Sleep(time.Millisecond * 10)
	lead.Stop()

	// state change
	go lead.Start()
	go lead.leader()
	lead.State.State(StateFollower)
	time.Sleep(time.Millisecond * 20)
	lead.Stop()

	// heartbeat tick
	lead.State.State(StateLeader)
	go lead.Start()
	go lead.leader()
	time.Sleep(time.Millisecond * 10)
	lead.Stop()

	wantErr := fmt.Errorf("test")
	lead.Lock()
	lead.OnLeader = func() error { return wantErr }
	lead.Unlock()
	err := lead.leader()
	equals(t, wantErr, err)
	time.Sleep(time.Millisecond * 10)
	equals(t, StateFollower, lead.State.State())
}
