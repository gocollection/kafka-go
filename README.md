Package **kafka_go** is an abstraction over popular kafka client sarama (https://github.com/Shopify/sarama).
Though sarama provides good enough APIs to integrate with a kafka cluster but still lags simplicity and
need a bit of domain knowledge even for a standard use case. End user has to maintain fail safety, reclaim
after re-balancing or similar scenarios, API doesn't seems very intuitive for the first time kafka users.
kafka_go tries to solves all such problems with its easy to understand APIs to start consuming from a kafka
cluster with significant less domain knowledge and complete fail safety.