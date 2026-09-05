package queuepool_test

import (
	"testing"
	"time"

	"github.com/solshuneo/queue-matching/queuepool"
)

func TestNewPool1v1(t *testing.T) {
	pool := queuepool.NewPool1v1()
	if pool == nil {
		t.Fatal("NewPool1v1 returned nil")
	}
}

var _ queuepool.Client = player{}

type player struct {
	id          int
	rating      int
	arrivalTime time.Time
}

func (p player) GetRating() int {
	return p.rating
}

func (p player) GetArrivalTime() time.Time {
	return p.arrivalTime
}

func (p player) GetIdentity() int {
	return p.id
}

func TestJoinPool1v1(t *testing.T) {
	pool := queuepool.NewPool1v1()

	player1 := player{id: 1, rating: 1000, arrivalTime: time.Now()}
	player2 := player{id: 2, rating: 1500, arrivalTime: time.Now()}
	ok1 := pool.Join(player1)
	if !ok1 {
		t.Fatalf("Join failed for player1")
	}

	clients := <-pool.Visualize()
	if len(clients) != 1 {
		t.Fatalf("Expected 1 client, got %d", len(clients))
	}
	if clients[0] != player1 {
		t.Fatalf("Expected player1, got %v", clients[0])
	}
	pool.Join(player2)
	clients = <-pool.Visualize()
	if len(clients) != 2 {
		t.Fatalf("Expected 2 clients, got %d", len(clients))
	}
	if clients[0] != player1 || clients[1] != player2 {
		t.Fatalf("Expected player1 and player2, got %v and %v", clients[0], clients[1])
	}
	pool.Join(player1)
	clients = <-pool.Visualize()
	if len(clients) != 2 {
		t.Fatalf("Expected 2 clients, got %d", len(clients))
	}
}

func TestLeavePool1v1(t *testing.T) {
	pool := queuepool.NewPool1v1()

	player1 := player{id: 1, rating: 1000, arrivalTime: time.Now()}
	player2 := player{id: 2, rating: 1500, arrivalTime: time.Now()}

	ok := pool.Join(player1)
	if !ok {
		t.Fatalf("Join failed for player1")
	}
	ok = pool.Join(player2)
	if !ok {
		t.Fatalf("Join failed for player2")
	}
	ok = pool.Leave(player1)
	if !ok {
		t.Fatalf("Leave failed for player1")
	}
	clients := <-pool.Visualize()
	if len(clients) != 1 {
		t.Fatalf("Expected 1 client, got %d", len(clients))
	}
	if clients[0] != player2 {
		t.Fatalf("Expected player2, got %v", clients[0])
	}
	ok = pool.Leave(player1)
	if !ok {
		t.Fatalf("Leave should fail for player1")
	}
	clients = <-pool.Visualize()
	if len(clients) != 1 {
		t.Fatalf("Expected 1 client, got %d", len(clients))
	}
	ok = pool.Leave(player2)
	if !ok {
		t.Fatalf("Leave should fail for player2")
	}
	clients = <-pool.Visualize()
	if len(clients) != 0 {
		t.Fatalf("Expected 0 clients, got %d", len(clients))
	}
}

func TestMatchPool1v1(t *testing.T) {
	// urgent solve
	atTime := time.Now()
	pool := queuepool.NewPool1v1()
	playerUrgent := player{id: 1, rating: 1000, arrivalTime: atTime.Add(-7 * time.Minute)}
	playerNormal := player{id: 2, rating: 1500, arrivalTime: atTime}
	_ = pool.Join(playerUrgent)
	_ = pool.Join(playerNormal)
	matcher := pool.GetMatch()
	pool.Matcher(atTime)
	clients := <-matcher
	if len(clients.Clients) != 2 {
		t.Fatalf("Expected 2 clients, got %d", len(clients.Clients))
	}
	if clients.Clients[0] != playerUrgent || clients.Clients[1] != playerNormal {
		t.Fatalf("Expected %v and %v, got %v and %v", playerUrgent, playerNormal, clients.Clients[0], clients.Clients[1])
	}
	left := <-pool.Visualize()
	if len(left) != 0 {
		t.Fatalf("Expected 0 clients, got %d", len(left))
	}
	// lazy test - need some pull requests :D
}
