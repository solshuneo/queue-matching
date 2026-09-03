package queuepool

const (
	poolCapacity = 100
)

type actionType int

var (
	actionJoin    = actionType(1)
	actionLeave   = actionType(2)
	actionMatcher = actionType(3)
)

type PairClient struct {
	Client1 Client
	Client2 Client
}

type Client interface {
	GetRating() int
}

type action struct {
	client Client
	action actionType
}
