kafka performance test

producer/consumer

[consumer]

java -jar kafka-test-client-1.0-SNAPSHOT.jar c {borker_ip:port} {topic} {consumer_id} {consumer_group}

[producer]

java -jar kafka-test-client-1.0-SNAPSHOT.jar p {broker_ip:port} {topic} {message_count} {tps} {message_byte_size}
