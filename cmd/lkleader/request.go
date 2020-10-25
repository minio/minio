package lkleader

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio/cmd/logger"
)

// HeartBeatRequest represents HeartBeat requests. Replication logging is ignored.
type HeartBeatRequest struct {
	Term     int
	LeaderID int
}

// heartBeatResponse represents the response to an heartBeat. In
// leader this always returns success.
type heartBeatResponse struct {
	Term    int
	Success bool
	Reason  string
}

// requestVoteRequest represents a requestVote sent by a candidate after an
// election timeout.
type requestVoteRequest struct {
	Term        int
	CandidateID int
}

// requestVoteResponse represents the response to a requestVote.
type requestVoteResponse struct {
	Term        int
	VoteGranted bool
	Reason      string
}

// RequestVoteRequest will broadcast a request for votes in order to update leader to
// either a follower or leader. If this candidate becomes leader error
// will return nil. The latest known term is always
// returned (this could be a newer term from another peer).
func (d *Leader) RequestVoteRequest() (int, error) {
	if d.State.State() == StateLeader {
		// no op
		if d.debug {
			logger.Info("already leader")
		}
		return d.State.Term(), nil
	}

	rvr := requestVoteRequest{
		Term:        d.State.Term(),
		CandidateID: d.State.ID(),
	}
	route := d.routePrefix + RouteRequestVote
	var body bytes.Buffer
	if err := gob.NewEncoder(&body).Encode(rvr); err != nil {
		logger.LogIf(context.Background(), err)
		return d.State.Term(), err
	}
	bodies := make([]io.Reader, len(d.Nodes))
	for i := range d.Nodes {
		bodies[i] = bytes.NewReader(body.Bytes())
	}

	responses := d.BroadcastRequest(d.Nodes, http.MethodPost, route, bodies, 0)
	votes := 0

retry:
	for i, resp := range responses {
		if resp == nil {
			// peer failed
			continue retry
		}
		var rvr requestVoteResponse
		if err := gob.NewDecoder(resp.Body).Decode(&rvr); err != nil {
			resp.Body.Close()
			logger.LogIf(context.Background(), err)
			continue retry
		}
		resp.Body.Close()

		if rvr.Term > d.State.Term() {
			// step down to a follower with the newer term
			logger.LogIf(context.Background(), fmt.Errorf("newer election term found %+v %s", rvr, d.State))
			return rvr.Term, ErrNewElectionTerm
		}

		if rvr.VoteGranted {
			if d.debug {
				logger.Info("vote granted from %s %+v", d.Nodes[i], rvr)
			}
			votes++
		}
	}

	expectedVotes := len(d.Nodes) / 2
	if len(d.Nodes)%2 == 0 {
		// When number of nodes are even
		// look for a higher majority.
		expectedVotes++
	}

	if votes < expectedVotes {
		if d.debug {
			logger.Info("too few votes %d but need more than %d", votes, expectedVotes)
		}
		return d.State.Term(), ErrTooFewVotes
	}

	if d.debug {
		logger.Info("election won with %d votes, becoming leader %s", votes, d.State)
	}
	return d.State.Term(), nil
}

// HeartBeatRequest will broadcast an HeartBeat request to peers.
// In the raft protocol this deals with appending and processing the
// replication log, however for leader election this is unused.
// It returns the current term with any errors.
func (d *Leader) HeartBeatRequest() (int, error) {
	hbr := HeartBeatRequest{
		Term:     d.State.Term(),
		LeaderID: d.State.ID(),
	}
	route := d.routePrefix + RouteHeartBeat
	var body bytes.Buffer
	if err := gob.NewEncoder(&body).Encode(hbr); err != nil {
		logger.LogIf(context.Background(), err)
		return d.State.Term(), err
	}
	bodies := make([]io.Reader, len(d.Nodes))
	for i := range d.Nodes {
		bodies[i] = bytes.NewReader(body.Bytes())
	}
	responses := d.BroadcastRequest(d.Nodes, http.MethodPost, route, bodies, 0)
	for _, resp := range responses {
		if resp == nil {
			// peer failed
			continue
		}
		var hbr heartBeatResponse
		if err := gob.NewDecoder(resp.Body).Decode(&hbr); err != nil {
			resp.Body.Close()
			logger.LogIf(context.Background(), err)
			continue
		}
		resp.Body.Close()
		if hbr.Term > d.State.Term() {
			if d.debug {
				logger.Info("newer election term found %+v", hbr)
			}
			return hbr.Term, ErrNewElectionTerm
		}
	}

	return d.State.Term(), nil
}

// BroadcastRequest will send a request to all other nodes in the system.
func (d *Leader) BroadcastRequest(peers []string, method, route string, bodies []io.Reader, timeoutMS int) []*http.Response {
	responses := make([]*http.Response, len(peers))
	var wg sync.WaitGroup
	wg.Add(len(peers))
	for ind, peer := range peers {
		go func(i int, p string) {
			defer wg.Done()

			url := p
			if !strings.HasPrefix(url, "http") {
				url = "http://" + url + route
			}
			req, err := http.NewRequest(method, url, bodies[i])
			if err != nil {
				logger.LogIf(context.Background(), err)
				return
			}
			if timeoutMS > 0 {
				ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutMS)*time.Millisecond)
				defer cancel()
				req = req.WithContext(ctx)
			}
			resp, err := d.client.Do(req)
			if err != nil {
				logger.LogIf(req.Context(), err)
				return
			}
			responses[i] = resp
		}(ind, peer)
	}
	wg.Wait()
	return responses
}
