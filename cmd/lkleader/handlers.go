package lkleader

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/minio/minio/cmd/logger"
)

const (
	// RouteID for id requests.
	RouteID = "/id"
	// RouteStatus will render the current nodes full state.
	RouteStatus = "/status"
	// RouteStepDown to force a node to step down.
	RouteStepDown = "/stepdown"
	// RouteRequestVote for request vote requests.
	RouteRequestVote = "/requestvote"
	// RouteHeartBeat for heart beat requests.
	RouteHeartBeat = "/heartbeat"
)

var (
	emptyHeartBeatResponse   bytes.Buffer
	emptyRequestVoteResponse bytes.Buffer
)

func init() {
	if err := gob.NewEncoder(&emptyHeartBeatResponse).Encode(heartBeatResponse{}); err != nil {
		log.Fatalln("error encoding gob:", err)
	}
	if err := gob.NewEncoder(&emptyRequestVoteResponse).Encode(requestVoteResponse{}); err != nil {
		log.Fatalln("error encoding gob:", err)
	}
}

// Route holds path and handler information.
type Route struct {
	Path    string
	Handler func(http.ResponseWriter, *http.Request)
}

// Routes create the routes required for leader election.
func (d *Leader) Router() *mux.Router {
	router := mux.NewRouter().SkipClean(true).UseEncodedPath()

	apiRouter := router.PathPrefix(d.routePrefix).Subrouter()

	apiRouter.Methods(http.MethodGet).Path(RouteID).HandlerFunc(d.IDHandler)
	apiRouter.Methods(http.MethodGet).Path(RouteStatus).HandlerFunc(d.StatusHandler)
	apiRouter.Methods(http.MethodPost).Path(RouteStepDown).HandlerFunc(d.StepDownHandler)
	apiRouter.Methods(http.MethodPost).Path(RouteRequestVote).HandlerFunc(d.RequestVoteHandler)
	apiRouter.Methods(http.MethodPost).Path(RouteHeartBeat).HandlerFunc(d.HeartBeatHandler)

	return apiRouter
}

// StepDownHandler (POST) will force the node to step down to a follower state.
func (d *Leader) StepDownHandler(w http.ResponseWriter, r *http.Request) {
	defer w.(http.Flusher).Flush()

	d.State.State(StateFollower)
	d.State.WriteTo(w)
}

// StatusHandler (GET) returns the nodes full state.
func (d *Leader) StatusHandler(w http.ResponseWriter, r *http.Request) {
	defer w.(http.Flusher).Flush()
	d.State.WriteTo(w)
}

// IDHandler (GET) returns the nodes id.
func (d *Leader) IDHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, d.State.ID())
}

// RequestVoteHandler (POST) handles requests for votes
func (d *Leader) RequestVoteHandler(w http.ResponseWriter, r *http.Request) {
	defer w.(http.Flusher).Flush()

	var rv requestVoteRequest
	if err := gob.NewDecoder(r.Body).Decode(&rv); err != nil {
		http.Error(w, emptyRequestVoteResponse.String(), http.StatusBadRequest)
		return
	}

	switch {
	case rv.Term < d.State.Term():
		// The request is from an old term so reject
		if d.debug {
			logger.Info("got RequestVote request from older term, rejecting %+v %s", rv, d.State)
		}
		rvResp := requestVoteResponse{
			Term:        d.State.Term(),
			VoteGranted: false,
			Reason:      fmt.Sprintf("term %d < %d", rv.Term, d.State.Term()),
		}
		if d.State.State() == StateLeader {
			rvResp.Reason = "already leader"
		}
		if err := gob.NewEncoder(w).Encode(rvResp); err != nil {
			http.Error(w, emptyRequestVoteResponse.String(), http.StatusInternalServerError)
		}
		return
	case (d.State.VotedFor() != NoVote) && (d.State.VotedFor() != rv.CandidateID):
		// don't double vote if already voted for a term
		if d.debug {
			logger.Info("got RequestVote request but already voted, rejecting %+v %s", rv, d.State)
		}
		rvResp := requestVoteResponse{
			Term:        d.State.Term(),
			VoteGranted: false,
			Reason:      fmt.Sprintf("already cast vote for %d", d.State.VotedFor()),
		}
		if err := gob.NewEncoder(w).Encode(rvResp); err != nil {
			http.Error(w, emptyRequestVoteResponse.String(), http.StatusInternalServerError)
		}
		return
	case rv.Term > d.State.Term():
		// Step down and reset state on newer term.
		if d.debug {
			logger.Info("got RequestVote request from newer term, stepping down %+v %s", rv, d.State)
		}
		d.State.StepDown(rv.Term)
		d.State.LeaderID(UnknownLeaderID)
	}

	// ok and vote for candidate
	// reset election timeout
	if d.debug {
		logger.Info("got RequestVote request, voting for candidate id %d %s", rv.CandidateID, d.State)
	}
	d.State.VotedFor(rv.CandidateID)
	// defer d.State.HeartbeatReset(true)
	rvResp := requestVoteResponse{
		Term:        d.State.Term(),
		VoteGranted: true,
	}

	if err := gob.NewEncoder(w).Encode(rvResp); err != nil {
		http.Error(w, emptyRequestVoteResponse.String(), http.StatusInternalServerError)
	}
}

// HeartBeatHandler (POST) handles append entry requests
func (d *Leader) HeartBeatHandler(w http.ResponseWriter, r *http.Request) {
	defer w.(http.Flusher).Flush()

	var hb HeartBeatRequest
	if err := gob.NewDecoder(r.Body).Decode(&hb); err != nil {
		http.Error(w, emptyRequestVoteResponse.String(), http.StatusBadRequest)
		return
	}

	// There are a few cases here:
	// 1. The request is from an old term so reject
	// 2. The request is from a newer term so step down
	// 3. If we are a follower and get an heart beat continue on with success
	// 4. If we are not a follower and get an heart beat, it means there is
	//    another leader already and we should step down.
	switch {
	case hb.Term < d.State.Term():
		if d.debug {
			logger.Info("got HeartBeat request from older term, rejecting %+v %s", hb, d.State)
		}
		hbResp := heartBeatResponse{
			Term:    d.State.Term(),
			Success: false,
			Reason:  fmt.Sprintf("term %d < %d", hb.Term, d.State.Term()),
		}
		if err := gob.NewEncoder(w).Encode(hbResp); err != nil {
			http.Error(w, emptyRequestVoteResponse.String(), http.StatusInternalServerError)
		}
		return
	case hb.Term > d.State.Term():
		if d.debug {
			logger.Info("agot HeartBeat request from newer term, stepping down %+v %s", hb, d.State)
		}
		d.State.StepDown(hb.Term)
	case hb.Term == d.State.Term():
		// ignore request to self and only step down if not in follower state.
		if hb.LeaderID != d.State.ID() && d.State.State() != StateFollower {
			if d.debug {
				logger.Info("got HeartBeat request from another leader with the same term, stepping down %+v %s", hb, d.State)
			}
			d.State.StepDown(hb.Term)
		}
	}

	// ok
	d.State.LeaderID(hb.LeaderID)
	d.State.HeartBeatEvent(&hb)
	hbResp := heartBeatResponse{
		Term:    d.State.Term(),
		Success: true,
	}

	if err := gob.NewEncoder(w).Encode(hbResp); err != nil {
		http.Error(w, emptyRequestVoteResponse.String(), http.StatusInternalServerError)
	}
}
