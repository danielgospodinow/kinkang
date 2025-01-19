package balancer

import (
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSmartBalanceBalancer_Balance(t *testing.T) {
	balancer := NewSmartBalanceBalancer()

	topic := "test-topic"
	numBrokers := int32(6)
	racks := map[int32][]int32{
		0: {0, 3},
		1: {1, 4},
		2: {2, 5},
	}
	assignments := map[int32][]int32{
		0:  {0, 1, 2},
		1:  {0, 1, 2},
		2:  {0, 1, 2},
		3:  {0, 1, 2},
		4:  {0, 1, 2},
		5:  {0, 1, 2},
		6:  {0, 1, 2},
		7:  {0, 1, 2},
		8:  {0, 1, 2},
		9:  {0, 1, 2},
		10: {0, 1, 2},
		11: {0, 1, 2},
	}

	newAssignments, err := balancer.Balance(topic, numBrokers, racks, assignments)
	if err != nil {
		t.Fatalf("Balance() returned an error: %v", err)
	}

	assert.True(t, checkRackCompliance(racks, newAssignments))
	assert.True(t, checkLeaderDistribution(numBrokers, newAssignments))
	assert.True(t, checkFollowerDistribution(numBrokers, newAssignments))
}

func checkRackCompliance(racks map[int32][]int32, assignments map[int32][]int32) bool {
	numRacks := len(racks)

	for partitionID, replicas := range assignments {
		rack0, rack1, rack2 := replicas[0]%int32(numRacks), replicas[1]%int32(numRacks), replicas[2]%int32(numRacks)
		if rack0 == rack1 || rack0 == rack2 || rack1 == rack2 {
			log.Printf("Partition %d is not rack compliant: %v\n", partitionID, replicas)
			return false
		}
	}
	return true
}

func checkLeaderDistribution(numBrokers int32, assignments map[int32][]int32) bool {
	leadersPerBroker := map[int32]int32{}
	for brokerID := range numBrokers {
		leadersPerBroker[brokerID] = 0
	}
	for _, replicaIDs := range assignments {
		leaderID := replicaIDs[0]
		leadersPerBroker[leaderID] += 1
	}

	minLeaderBrokerID, maxLeaderBrokerID := int32(0), int32(0)
	for brokerID, leaderCount := range leadersPerBroker {
		if leaderCount < leadersPerBroker[minLeaderBrokerID] {
			minLeaderBrokerID = brokerID
		}
		if leaderCount > leadersPerBroker[maxLeaderBrokerID] {
			maxLeaderBrokerID = brokerID
		}
	}

	log.Printf("Leaders per broker: %v\n", leadersPerBroker)
	return leadersPerBroker[maxLeaderBrokerID]-leadersPerBroker[minLeaderBrokerID] <= 1
}

func checkFollowerDistribution(numBrokers int32, assignments map[int32][]int32) bool {
	followersPerBroker := map[int32]int32{}
	for brokerID := range numBrokers {
		followersPerBroker[brokerID] = 0
	}
	for _, replicas := range assignments {
		followersPerBroker[replicas[1]] += 1
		followersPerBroker[replicas[2]] += 1
	}

	minFollowerBrokerID, maxFollowerBrokerID := int32(0), int32(0)
	for brokerID, followerCount := range followersPerBroker {
		if followerCount < followersPerBroker[minFollowerBrokerID] {
			minFollowerBrokerID = brokerID
		}
		if followerCount > followersPerBroker[maxFollowerBrokerID] {
			maxFollowerBrokerID = brokerID
		}
	}

	log.Printf("Followers per broker: %v\n", followersPerBroker)
	return followersPerBroker[maxFollowerBrokerID]-followersPerBroker[minFollowerBrokerID] <= 2
}
