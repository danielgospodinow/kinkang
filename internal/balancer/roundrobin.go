package balancer

type roundRobinBalancer struct {
}

var _ Balancer = (*roundRobinBalancer)(nil)

// NewRoundRobinBalancer creates a new Round Robin balancer.
func NewRoundRobinBalancer() *roundRobinBalancer {
	return &roundRobinBalancer{}
}

// Balance balances a given Kafka topic based on its current assignments using the Round Robin algorithm.
//
// NOTE: This algorithm assumes broker IDs are 0-indexed and contiguous.
//
// NOTE: This implementation of the algorithm might concentrate replicas on the first couple of brokers since
// all topic are always spread from the first broker onwards. This is a bug logged in https://github.com/danielgospodinow/kinkang/issues/2.
func (b *roundRobinBalancer) Balance(topic string, numBrokers int32, racks map[int32][]int32, currentAssignments map[int32][]int32) (map[int32][]int32, error) {
	newAssignments := map[int32][]int32{}

	numPartitions := len(currentAssignments)
	for partitionIndex := range numPartitions {
		partitionIndex := int32(partitionIndex)
		replicationFactor := len(currentAssignments[partitionIndex])

		newPartitionAssignment := []int32{}
		for replicaIndex := range replicationFactor {
			replicaIndex := int32(replicaIndex)
			newPartitionAssignment = append(newPartitionAssignment, (partitionIndex+replicaIndex)%numBrokers)
		}

		newAssignments[partitionIndex] = newPartitionAssignment
	}

	return newAssignments, nil
}
