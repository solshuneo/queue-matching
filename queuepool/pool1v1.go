package queuepool

import (
	"errors"
	"slices"
	"time"
)

var ErrStillUrgentNotSolved = errors.New("Urgent not solved yet")

type Pool1v1 struct {
	action chan action
	queue  []Client
	match  chan *Clients
}

func NewPool1v1() *Pool1v1 {
	return &Pool1v1{
		queue:  make([]Client, 0, poolCapacity),
		action: make(chan action, poolCapacity),
		match:  make(chan *Clients, poolCapacity),
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

func (p *Pool1v1) removeClientByID(id int) {
	if id < 0 || id >= len(p.queue) {
		return
	}
	if id == len(p.queue)-1 {
		p.queue = p.queue[:id]
	} else {
		p.queue = append(p.queue[:id], p.queue[id+1:]...)
	}
}

func (p *Pool1v1) removeClient(client Client) {
	for i, c := range p.queue {
		if c == client {
			p.removeClientByID(i)
			return
		}
	}
}

func (p *Pool1v1) matcher() {
	// urgent solve first
	for i := 0; i < len(p.queue); {
		if isStatus(p.queue[i]) == Urgent {
			if i == len(p.queue)-1 {
				continue
			}
			res := &Clients{
				Clients: []Client{p.queue[i], p.queue[i+1]},
			}
			p.removeClientByID(i)
			p.removeClientByID(i + 1)
			p.match <- res
		} else {
			i += 1
		}
	}
	for i := 0; i < len(p.queue); {
		delta, err := delta(p.queue[i])
		if err != nil {
			continue
		}
		for j := i + 1; j < len(p.queue); j++ {
			if p.queue[i].GetRating()-delta < p.queue[j].GetRating() && p.queue[j].GetRating() < p.queue[i].GetRating()+delta {
				res := &Clients{
					Clients: []Client{p.queue[i], p.queue[j]},
				}
				p.removeClientByID(i)
				p.removeClientByID(i + 1)
				p.match <- res
				break
			} else {
				if j == len(p.queue)-1 {
					i += 1
				}
			}
		}
	}
}

// exported field

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

func (p *Pool1v1) GetMatch() <-chan *Clients {
	return p.match
}

// helper
type Status int

var (
	Urgent = Status(1)
	Normal = Status(2)
	Early  = Status(3)
)

func isStatus(client Client) Status {
	t := client.GetArrivalTime()
	if time.Since(t) >= 7*time.Minute {
		return Urgent
	}
	if time.Since(t) <= 2*time.Microsecond {
		return Early
	}
	return Normal
}

func delta(client Client) (int, error) {
	steps := make(map[time.Duration]int)
	steps[10*time.Second] = 5
	steps[20*time.Second] = 10
	steps[30*time.Second] = 20
	steps[40*time.Second] = 30
	steps[50*time.Second] = 40
	steps[1*time.Minute] = 50
	steps[2*time.Minute] = 100
	steps[3*time.Minute] = 200
	steps[4*time.Minute] = 300
	steps[5*time.Minute] = 400
	steps[6*time.Minute] = 500
	steps[7*time.Minute] = 600
	for _, v := range steps {
		if int(time.Since(client.GetArrivalTime())) <= v {
			return v, nil
		}
	}
	return -1, ErrStillUrgentNotSolved
}
