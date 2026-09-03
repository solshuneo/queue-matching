package queuepool

import (
	"slices"
)

type Pool1v1 struct {
	action chan action
	queue  []Client
	match  chan *PairClient
}

func NewPool1v1() *Pool1v1 {
	return &Pool1v1{
		queue:  make([]Client, 0, poolCapacity),
		action: make(chan action, poolCapacity),
		match:  make(chan *PairClient, poolCapacity),
	}
}

func (p *Pool1v1) Start() {
	go func() {
		for {
			act := <-p.action
			switch act.action {
			case actionJoin:
				p.addClient(act.client)
			case actionLeave:
				p.removeClient(act.client)
			case actionMatcher:
				p.matcher()
			}

		}
	}()
}

func (p *Pool1v1) addClient(client Client) {
	if slices.Contains(p.queue, client) {
		return
	}
	p.queue = append(p.queue, client)
}

func (p *Pool1v1) removeClient(client Client) {
	for i, c := range p.queue {
		if c == client {
			if i == len(p.queue)-1 {
				p.queue = p.queue[:i]
			} else {
				p.queue = append(p.queue[:i], p.queue[i+1:]...)
			}
			return
		}
	}
}

func (p *Pool1v1) matcher() {
	slices.SortFunc(p.queue, func(a, b Client) int {
		return a.GetRating() - b.GetRating()
	})
	for i := 0; i < len(p.queue)-1; {
		if p.queue[i].GetRating() == p.queue[i+1].GetRating() {
			res := &PairClient{
				Client1: p.queue[i],
				Client2: p.queue[i+1],
			}
			p.removeClient(res.Client1)
			p.removeClient(res.Client2)
			p.match <- res
		} else {
			i++
		}
	}
}

func (p *Pool1v1) Join(client Client) {
	p.action <- action{client: client, action: actionJoin}
}

func (p *Pool1v1) Leave(client Client) {
	p.action <- action{client: client, action: actionLeave}
}

func (p *Pool1v1) Matcher() {
	p.action <- action{action: actionMatcher}
}

func (p *Pool1v1) Visualize() []Client {
	clients := make([]Client, len(p.queue))
	copy(clients, p.queue)
	return clients
}

func (p *Pool1v1) GetMatch() <-chan *PairClient {
	return p.match
}
