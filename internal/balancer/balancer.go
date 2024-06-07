package balancer

// Balancer is the interface describing a Kafka topic balancer.
type Balancer interface {
	// Balance a given Kafka topic based on its current assignments.
	Balance(topic string, numBrokers int32, assignments map[int32][]int32) (map[int32][]int32, error)
}
