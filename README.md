# Kinkang

Kinkang, i.e. Yinyang for Kafka, is a simple Kafka topic balancer written in Golang.

It uses IBM's `sarama` library to interact with Kafka.

The types of supported balancing are:

* Round Robin
* Smart Balance

This piece of software can be used as

* (TBD) A Kubernetes `cronjob` that periodically balances clusters.
* (TBD) A CLI tool that can be used for on-demand cluster balancing.
* A standalone app to trigger on-demand cluster balancing.
