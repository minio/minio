package lkleader

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/env"
)

var (
	// DefaultOnLeader is a no op function to execute when a node becomes a leader.
	DefaultOnLeader = func() error { return nil }
	// DefaultOnFollower is a no op function to execute when a node becomes a follower.
	DefaultOnFollower = func() error { return nil }
	// DefaultRoutePrefix is what is prefixed for the leader routes. (/leader)
	DefaultRoutePrefix = "/minio/lock/v1"

	// ErrTooFewVotes happens on a RequestVote when the candidate receives less than the
	// majority of votes.
	ErrTooFewVotes = errors.New("too few votes")
	// ErrNewElectionTerm if during RequestVote there is a higher term found.
	ErrNewElectionTerm = errors.New("newer election term")
	// ErrLeader is returned when an operation can't be completed on a
	// leader node.
	ErrLeader = errors.New("node is the leader")
	// ErrNotLeader is returned when an operation can't be completed on a
	// follower or candidate node.
	ErrNotLeader = errors.New("node is not the leader")
)

// Leader manages the raft FSM and executes OnLeader and OnFollower events.
type Leader struct {
	sync.Mutex

	client *http.Client
	// routePrefix will be prefixed to all handler routes. This should start with /route.
	routePrefix string
	stopChan    chan struct{}
	debug       bool

	// Addr is a host:port for the current node.
	Addr string
	// Nodes is a list of all nodes for consensus.
	Nodes []string
	// OnLeader is an optional function to execute when becoming a leader.
	OnLeader func() error
	// OnFollower is an optional function to execute when becoming a follower.
	OnFollower func() error
	// State for holding the raft state.
	State *State
}

// CallbackFunc is for on leader and on follower events.
type CallbackFunc func() error

// New initializes a new leader. Start is required to be run to
// begin leader election.
func New(addr string, nodes []string, onLeader, onFollower CallbackFunc, eMS, hMS int) (*Leader, error) {
	if onLeader == nil {
		onLeader = DefaultOnLeader
	}

	if onFollower == nil {
		onFollower = DefaultOnFollower
	}

	c := &http.Client{
		Timeout: time.Duration(hMS) * time.Millisecond,
	}

	id, err := hashToInt(addr, len(nodes)*1000)
	if err != nil {
		return nil, err
	}
	id++
	d := &Leader{
		client:      c,
		routePrefix: DefaultRoutePrefix,
		stopChan:    make(chan struct{}),
		debug:       env.Get("MINIO_LEADER_ELECTION_DEBUG", "off") == "on",
		Addr:        addr,
		Nodes:       nodes,
		OnLeader:    onLeader,
		OnFollower:  onFollower,
		State:       NewState(id, eMS, hMS),
	}
	return d, nil
}

// Stop will stop any running event loop.
func (d *Leader) Stop() error {
	if d.debug {
		logger.Info("stopping event loop")
	}
	// exit any state running and the main event fsm.
	for i := 0; i < 2; i++ {
		d.stopChan <- struct{}{}
	}
	return nil
}

// Start begins the leader election process.
func (d *Leader) Start() error {
	if d.debug {
		logger.Info("starting event loop")
	}
	for {
		if d.debug {
			logger.Info(d.State.String())
		}
		select {
		case <-d.stopChan:
			if d.debug {
				logger.Info("stopping event loop")
			}
			return nil
		default:
		}
		switch d.State.State() {
		case StateFollower:
			if err := d.follower(); err != nil {
				return err
			}
		case StateCandidate:
			d.candidate()
		case StateLeader:
			if err := d.leader(); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unknown state %d", d.State.State())
		}
	}
}

// follower will wait for an HeartBeat from the leader and on expiration will begin
// the process of leader election with a RequestVote.
func (d *Leader) follower() error {
	if d.debug {
		logger.Info("entering follower state, leader id", d.State.LeaderID())
	}
	if err := d.OnFollower(); err != nil {
		logger.LogIf(context.Background(), err)
		return err
	}

retry:
	for {
		select {
		case <-d.stopChan:
			return nil
		case newState := <-d.State.StateChanged():
			if newState == StateFollower {
				continue
			}
			if d.debug {
				logger.Info("follower state changed to", d.State.StateString(newState))
			}
			return nil
		case <-d.State.HeartbeatReset():
			if d.debug {
				logger.Info("heartbeat reset")
			}
			continue retry
		case hbr := <-d.State.HeartBeatEvent():
			if d.debug {
				logger.Info(d.State.StateString(d.State.State()), "got HeartBeat from leader", hbr)
			}
			continue retry
		case h := <-d.State.HeartbeatTickRandom():
			// https://raft.github.io/raft.pdf
			// If a follower receives no communication over a period of time
			// called the election timeout, then it assumes there is no viable
			// leader and begins an election to choose a new leader.
			// To begin an election, a follower increments its current
			// term and transitions to candidate state.
			if d.debug {
				logger.Info("follower heartbeat timeout, transitioning to candidate", h)
			}
			d.State.VotedFor(NoVote)
			d.State.LeaderID(UnknownLeaderID)
			d.State.Term(d.State.Term() + 1)
			d.State.State(StateCandidate)
			return nil
		}
	}
}

// candidate is for when in StateCandidate. The loop will
// attempt an election repeatedly until it receives events.
// https://raft.github.io/raft.pdf
//
// A candidate continues in this state until one of three things happens:
// (a) it wins the election
// (b) another server establishes itself as leader, or
// (c) a period of time goes by with no winner.
func (d *Leader) candidate() {
	if d.debug {
		logger.Info("entering candidate state")
	}
	go func() {
		if d.debug {
			logger.Info("requesting vote")
		}
		currentTerm, err := d.RequestVoteRequest()
		if err != nil {
			logger.LogIf(context.Background(), err)
			switch err {
			case ErrNewElectionTerm:
				d.State.StepDown(currentTerm)
			case ErrTooFewVotes:
				d.State.State(StateFollower)
			}
			return
		}
		// it wins the election
		d.State.LeaderID(d.State.ID())
		d.State.State(StateLeader)
	}()
	for {
		select {
		case <-d.stopChan:
			return
		case hbr := <-d.State.HeartBeatEvent():
			// https://raft.github.io/raft.pdf
			// While waiting for votes, a candidate may receive an
			// HeartBeat RPC from another server claiming to be
			// leader. If the leader’s term (included in its RPC) is at least
			// as large as the candidate’s current term, then the candidate
			// recognizes the leader as legitimate and returns to follower
			// state. If the term in the RPC is smaller than the candidate’s
			// current term, then the candidate rejects the RPC and continues
			// in candidate state.
			if d.debug {
				logger.Info("candidate got an HeartBeat from a leader", hbr)
			}
			if hbr.Term >= d.State.Term() {
				d.State.StepDown(hbr.Term)
				return
			}
		case newState := <-d.State.StateChanged():
			if newState == StateCandidate {
				continue
			}
			if d.debug {
				logger.Info("candidate state changed to", d.State.StateString(newState))
			}
			return
		case e := <-d.State.ElectionTick():
			if d.debug {
				logger.Info("election timeout, restarting election", e)
			}
			return
		}
	}
}

// leader is for when in StateLeader. The loop will continually send
// a heartbeat of HeartBeat to all peers at a rate of HeartbeatTimeoutMS.
func (d *Leader) leader() error {
	if d.debug {
		logger.Info("entering leader state")
	}
	go d.HeartBeatRequest()
	errChan := make(chan error)
	go func() {
		// Run the OnLeader event in a goroutine in case
		// it has a long delay. Any errors returned will exit the
		// leader state.
		d.Lock()
		defer d.Unlock()
		if err := d.OnLeader(); err != nil {
			logger.LogIf(context.Background(), err)
			errChan <- err
		}
	}()
	for {
		select {
		case err := <-errChan:
			d.State.State(StateFollower)
			go func() {
				// Removing the state change event here
				// before returning error.
				<-d.State.StateChanged()
			}()
			return err
		case <-d.stopChan:
			return nil
		case <-d.State.HeartBeatEvent():
			// ignore any append entries to self.
			continue
		case newState := <-d.State.StateChanged():
			if newState == StateLeader {
				continue
			}
			if d.debug {
				logger.Info("leader state changed to", d.State.StateString(newState))
			}
			return nil
		case h := <-d.State.HeartbeatTick():
			if d.debug {
				logger.Info("sending to peers HeartBeatRequest", d.Nodes, h)
			}
			currentTerm, err := d.HeartBeatRequest()
			if err != nil {
				logger.LogIf(context.Background(), err)
				switch err {
				case ErrNewElectionTerm:
					d.State.StepDown(currentTerm)
					return nil
				}
			}
		}
	}
}
