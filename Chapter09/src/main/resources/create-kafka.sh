#!/usr/bin/env bash

kafka-topics --delete --zookeeper localhost:2181 --topic gzet
kafka-server-start /usr/local/etc/kafka/server.properties > /var/log/kafka/kafka-server.log 2>&1 &
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic gzet
