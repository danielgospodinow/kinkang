package main

import (
	"log"
	"os"

	"github.com/IBM/sarama"
	"github.com/danielgospodinow/kinkang/internal/balancer"
)

const (
	kafkaBootstrapServerConfig = "KAFKA_BOOTSTRAP_SERVER"
	kafkaUserConfig            = "KAFKA_USER"
	kafkaPasswordConfig        = "KAFKA_PASSWORD"

	balancerAlgorithmConfig       = "BALANCER_ALGORITHM"
	balancerAlgorithmRoundRobin   = "round-robin"
	balancerAlgorithmSmartBalance = "smart-balance"
)

var (
	defaultBalancerAlgorithm = balancer.NewRoundRobinBalancer()
)

func main() {
	kafkaBootstrapServer := os.Getenv(kafkaBootstrapServerConfig)
	kafkaUser := os.Getenv(kafkaUserConfig)
	kafkaPassword := os.Getenv(kafkaPasswordConfig)

	if kafkaBootstrapServer == "" || kafkaUser == "" || kafkaPassword == "" {
		log.Fatalf("Missing required environment variables: %s, %s, %s", kafkaBootstrapServerConfig, kafkaUserConfig, kafkaPasswordConfig)
	}

	saramaCfg := sarama.NewConfig()
	saramaCfg.Version = sarama.V3_5_0_0
	saramaCfg.Net.SASL.Enable = true
	saramaCfg.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	saramaCfg.Net.SASL.User = kafkaUser
	saramaCfg.Net.SASL.Password = kafkaPassword
	saramaCfg.Net.TLS.Enable = true

	admin, err := sarama.NewClusterAdmin([]string{kafkaBootstrapServer}, saramaCfg)
	if err != nil {
		log.Fatalf("Error creating cluster admin: %v", err)
	}

	brokers, _, err := admin.DescribeCluster()
	if err != nil {
		log.Fatalf("Error describing cluster: %v", err)
	}

	numBrokers := len(brokers)
	log.Printf("Cluster has %d brokers\n", numBrokers)

	topicDetails, err := admin.ListTopics()
	if err != nil {
		log.Fatalf("Error listing topics: %v", err)
	}

	balancingAlgorithm := os.Getenv(balancerAlgorithmConfig)
	var topicBalancer balancer.Balancer
	switch balancingAlgorithm {
	case balancerAlgorithmRoundRobin:
		log.Printf("Using Round Robin balancer algorithm\n")
		topicBalancer = balancer.NewRoundRobinBalancer()
	case balancerAlgorithmSmartBalance:
		log.Printf("Using Smart Balance balancer algorithm\n")
		topicBalancer = balancer.NewSmartBalanceBalancer()
	default:
		log.Printf("Using default balancer algorithm\n")
		topicBalancer = defaultBalancerAlgorithm
	}

	for topic, details := range topicDetails {
		log.Printf("Looking at topic: '%s', assignments: %v\n", topic, details.ReplicaAssignment)

		log.Printf("Balancing topic: '%s'...\n", topic)
		newAssignments, err := topicBalancer.Balance(topic, int32(numBrokers), details.ReplicaAssignment)
		if err != nil {
			log.Printf("Error balancing topic: %v, skipping it...", err)
		}

		log.Printf("Found new assignments for topic '%s': %v\n", topic, newAssignments)

		err = admin.AlterPartitionReassignments(topic, balancer.ConvertAssignmetsMapToMatrix(newAssignments))
		if err != nil {
			log.Printf("Error while submitting new assignments to Kafka for topic: %v, skipping it...", err)
			continue
		}

		log.Printf("Topic '%s' successfully rebalanced\n", topic)
	}
}
