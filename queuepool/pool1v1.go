package queuepool

import (
	"errors"
	"reflect"
	"sync"
	"time"
)

var ErrStillUrgentNotSolved = errors.New("Urgent not solved yet")

type Pool1v1 struct {
	action    chan action
	queue     []*Client
	match     chan *Clients
	visualize chan []Client
	identity  sync.Map
}

func NewPool1v1() *Pool1v1 {
	pool := &Pool1v1{
		queue:     make([]*Client, 0, poolCapacity),
		action:    make(chan action, poolCapacity),
		match:     make(chan *Clients, poolCapacity),
		visualize: make(chan []Client),
	}
	pool.start()
	return pool
}

func (p *Pool1v1) start() {
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
			case actionVisualize:
				clients := make([]Client, 0, len(p.queue))
				for _, c := range p.queue {
					clients = append(clients, *c)
				}
				p.visualize <- clients
			}
		}
	}()
}

func (p *Pool1v1) addClient(client Client) {
	if _, ok := p.identity.Load(client.GetIdentity()); ok {
		return
	}
	p.identity.Store(client.GetIdentity(), true)
	p.queue = append(p.queue, &client)
}

func (p *Pool1v1) removeClient(client Client) {
	if _, ok := p.identity.Load(client.GetIdentity()); !ok {
		return
	}
	for id := 0; id < len(p.queue); id++ {
		if (*p.queue[id]).GetIdentity() == client.GetIdentity() {
			if id == len(p.queue)-1 {
				p.queue = p.queue[:id]
			} else {
				p.queue = append(p.queue[:id], p.queue[id+1:]...)
			}
			break
		}
	}
	p.identity.Delete(client.GetIdentity())
}

func (p *Pool1v1) matcher() {
	// urgent solve first
	for i := 0; i < len(p.queue); {
		if isStatus(p.queue[i]) == Urgent {
			if i == len(p.queue)-1 {
				continue
			}
			res := &Clients{
				Clients: []Client{*p.queue[i], *p.queue[i+1]},
			}
			p.removeClient(res.Clients[0])
			p.removeClient(res.Clients[1])
			p.match <- res
		} else {
			break
		}
	}
	for i := 0; i < len(p.queue); {
		delta, err := delta(p.queue[i])
		if err != nil {
			continue
		}
		for j := i + 1; j < len(p.queue); j++ {
			irating := (*p.queue[i]).GetRating()
			jrating := (*p.queue[j]).GetRating()
			if irating-delta < jrating && jrating < irating+delta {
				res := &Clients{
					Clients: []Client{*p.queue[i], *p.queue[j]},
				}
				p.removeClient(res.Clients[0])
				p.removeClient(res.Clients[1])
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

func (p *Pool1v1) Join(client Client) bool {
	if reflect.TypeOf(client).Kind() == reflect.Pointer {
		return false
	}
	p.action <- action{client: client, action: actionJoin}
	return true
}

func (p *Pool1v1) Leave(client Client) bool {
	if reflect.TypeOf(client).Kind() == reflect.Pointer {
		return false
	}
	p.action <- action{client: client, action: actionLeave}
	return true
}

func (p *Pool1v1) Matcher() {
	p.action <- action{action: actionMatcher}
}

func (p *Pool1v1) Visualize() <-chan []Client {
	p.action <- action{action: actionVisualize}
	return p.visualize
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

func isStatus(client *Client) Status {
	t := (*client).GetArrivalTime()
	if time.Since(t) >= 7*time.Minute {
		return Urgent
	}
	if time.Since(t) <= 2*time.Microsecond {
		return Early
	}
	return Normal
}

func delta(client *Client) (int, error) {
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
		if int(time.Since((*client).GetArrivalTime())) <= v {
			return v, nil
		}
	}
	return -1, ErrStillUrgentNotSolved
}
