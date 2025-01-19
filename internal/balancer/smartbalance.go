package balancer

import (
	"errors"
	"log"
)

type smartBalanceBalancer struct {
}

var _ Balancer = (*smartBalanceBalancer)(nil)

// NewSmartBalanceBalancer creates a new Smart Balance balancer.
func NewSmartBalanceBalancer() *smartBalanceBalancer {
	return &smartBalanceBalancer{}
}

// Balance balances a given Kafka topic based on its current assignments using the Smart Balance algorithm. It guarantees rack compliance for partitions.
//
// The algorithm is as follows:
//
// 1. Find the per-broker avg of leaders for the given topic, i.e. the desired state for leader distribution. E.g. if a topic has 12 partitions and there are 3 brokers, then each broker should have ideally have 4 leaders.
//
// 2. Find the leaders per broker for that topic.
//
// 3. Move leaders from the top brokers to the bottom brokers until the leaders per broker are balanced, i.e. diff between min and max brokers is <= 1.
//
// 4. On each leader movement, we get the destination rack and make sure we put the follower replicas in the other 2 racks. This is to ensure rack compliance. We can put the followers in each rack on the broker that has the least amount of follower replicas for that topic.
//
// NOTE: This algorithm assumes broker IDs are 0-indexed and contiguous.
//
// NOTE: This algorithm assumes a replication factor of 3 for all topics.
//
// NOTE: This algorithm assumes a total of 3 racks.
func (b *smartBalanceBalancer) Balance(topic string, numBrokers int32, racks map[int32][]int32, assignments map[int32][]int32) (map[int32][]int32, error) {
	if len(racks) != 3 {
		return nil, errors.New("expected 3 racks")
	}
	if len(assignments[0]) != 3 {
		return nil, errors.New("expected replication factor of 3")
	}
	if numBrokers%3 != 0 {
		return nil, errors.New("expected brokers to be a multiple of 3")
	}

	log.Printf("Balancing topic: %s\n", topic)
	log.Printf("Num of racks: %d\n", len(racks))
	log.Printf("Initial assignments for topic %s: %v\n", topic, assignments)

	avgLeadersPerBroker := int32(len(assignments)) / numBrokers
	log.Printf("Avg leaders per broker: %d\n", avgLeadersPerBroker)

	for partitionID, replicas := range assignments {
		leadersPerBroker := map[int32]int32{}
		for brokerID := range numBrokers {
			leadersPerBroker[brokerID] = 0
		}
		for _, replicas := range assignments {
			leaderID := replicas[0]
			leadersPerBroker[leaderID] += 1
		}
		log.Printf("Current leaders per broker: %v\n", leadersPerBroker)

		log.Printf("Looking at partition: %d, replicas: %v\n", partitionID, replicas)

		leaderBrokerID := replicas[0]
		if leadersPerBroker[leaderBrokerID] <= avgLeadersPerBroker {
			log.Printf("Leader broker %d has less than or equal to avg leaders, skipping it...\n", leaderBrokerID)
			continue
		}

		log.Printf("Leader broker %d has more than avg leaders (ie more than %d): %d, moving leader to a different broker...\n", leaderBrokerID, avgLeadersPerBroker, leadersPerBroker[leaderBrokerID])

		minLeaderBrokerID := int32(0)
		for brokerID, leaderCount := range leadersPerBroker {
			if leaderCount < leadersPerBroker[minLeaderBrokerID] {
				minLeaderBrokerID = brokerID
			}
		}
		log.Printf("Current min leader broker: %d with %d leaders\n", minLeaderBrokerID, leadersPerBroker[minLeaderBrokerID])

		if leadersPerBroker[leaderBrokerID]-leadersPerBroker[minLeaderBrokerID] <= 1 {
			log.Printf("Difference between current leader count and min leader count is <= 1, skipping this reassignment...\n")
			continue
		}

		log.Printf("Moving leader for partition %d from broker %d to broker %d...\n", partitionID, leaderBrokerID, minLeaderBrokerID)
		assignments[partitionID][0] = minLeaderBrokerID

		rackOfLeader, _ := rackOfBroker(minLeaderBrokerID, racks)
		rackOfFollowerOne := (rackOfLeader + 1) % int32(len(racks))
		rackOfFollowerTwo := (rackOfLeader + 2) % int32(len(racks))
		log.Printf("Leader is in rack: %d, moving followers to other racks: %d, %d...\n", rackOfLeader, rackOfFollowerOne, rackOfFollowerTwo)
		assignments[partitionID][1] = minFollowersBrokerForRack(rackOfFollowerOne, racks, assignments)
		assignments[partitionID][2] = minFollowersBrokerForRack(rackOfFollowerTwo, racks, assignments)

		log.Printf("New assignment for partition %d: %v\n", partitionID, assignments[partitionID])
	}

	if !isRackComplianceMet(racks, assignments) {
		return nil, errors.New("rack compliance is not met")
	}

	log.Printf("Final assignments for topic %s: %v\n", topic, assignments)
	return assignments, nil
}

// minFollowersBrokerForRack returns the broker with the least amount of follower replicas for a partition for the given rack.
func minFollowersBrokerForRack(rack int32, racks map[int32][]int32, assignments map[int32][]int32) int32 {
	followerCounts := map[int32]int32{}
	for _, brokerID := range racks[rack] {
		followerCounts[brokerID] = 0
	}
	for _, replicas := range assignments {
		if _, ok := followerCounts[replicas[1]]; ok {
			followerCounts[replicas[1]] += 1
		}
		if _, ok := followerCounts[replicas[2]]; ok {
			followerCounts[replicas[2]] += 1
		}
	}

	minFollowerBrokerID := racks[rack][0]
	for _, brokerID := range racks[rack] {
		if followerCounts[brokerID] < followerCounts[minFollowerBrokerID] {
			minFollowerBrokerID = brokerID
		}
	}

	return minFollowerBrokerID
}

// isRackComplianceMet checks if all topic partitions are well spread across racks.
func isRackComplianceMet(racks map[int32][]int32, assignments map[int32][]int32) bool {
	numRacks := int32(len(racks))

	for partitionID, replicas := range assignments {
		rack0, rack1, rack2 := replicas[0]%numRacks, replicas[1]%numRacks, replicas[2]%numRacks
		if rack0 == rack1 || rack0 == rack2 || rack1 == rack2 {
			log.Printf("Partition %d is not rack compliant: %v\n", partitionID, replicas)
			return false
		}
	}
	return true
}

// rackOfBroker returns the rack of the given broker.
func rackOfBroker(brokerID int32, racks map[int32][]int32) (int32, error) {
	for rack, brokerIDs := range racks {
		for _, id := range brokerIDs {
			if id == brokerID {
				return rack, nil
			}
		}
	}
	return -1, errors.New("broker not found in any rack")
}
