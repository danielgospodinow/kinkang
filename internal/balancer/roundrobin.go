package balancer

type roundRobinBalancer struct {
}

var _ Balancer = (*roundRobinBalancer)(nil)

// NewRoundRobinBalancer creates a new Round Robin balancer.
func NewRoundRobinBalancer() *roundRobinBalancer {
	return &roundRobinBalancer{}
}

// Balance balances a given Kafka topic based on its current assignments using the Round Robin algorithm.
func (b *roundRobinBalancer) Balance(topic string, currentAssignments map[int32][]int32) (map[int32][]int32, error) {
	// TODO: Implement.
	return currentAssignments, nil
}
