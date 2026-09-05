package queuepool

import "time"

const (
	poolCapacity = 100
)

type actionType int

var (
	actionJoin      = actionType(1)
	actionLeave     = actionType(2)
	actionMatcher   = actionType(3)
	actionVisualize = actionType(4)
)

type Clients struct {
	Clients []Client
}

type Client interface {
	GetIdentity() int
	GetRating() int
	GetArrivalTime() time.Time
}

type action struct {
	client Client
	action actionType
}
