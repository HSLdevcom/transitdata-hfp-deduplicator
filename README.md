[![Build Status](https://travis-ci.org/HSLdevcom/transitdata-hfp-deduplicator.svg?branch=master)](https://travis-ci.org/HSLdevcom/transitdata-hfp-deduplicator)

# Transitdata-hfp-deduplicator

## Description

Application for de-duplicating HFP Messages read from single/multiple Pulsar topics.
Writes de-duplicated output to another Pulsar topic.

## Building

### Dependencies

This project depends on [transitdata-common](https://github.com/HSLdevcom/transitdata-common) project.

### Locally

- ```mvn compile```  
- ```mvn package```  

### Docker image

- Run [this script](build-image.sh) to build the Docker image


## Tests:

We're separating our unit & integration tests using [this pattern](https://www.petrikainulainen.net/programming/maven/integration-testing-with-maven/).

Unit tests:

- add test classes under ./src/test with suffix *Test.java
- `mvn clean test -P unit-test`   

Integration tests:

- add test classes under ./src/integration-test with prefix IT*.java
- `mvn clean verify -P integration-test`   


## Running

Requirements:
- Local Pulsar Cluster
  - By default uses localhost, override host in PULSAR_HOST if needed.
    - Tip: f.ex if running inside Docker in OSX set `PULSAR_HOST=host.docker.internal` to connect to the parent machine
  - You can use [this script](https://github.com/HSLdevcom/transitdata/blob/master/bin/pulsar/pulsar-up.sh) to launch it as Docker container

Launch Docker container with

```docker-compose -f compose-config-file.yml up <service-name>```   

## Configuration

The application is configured using environment variables.

| Environment variable                      | Required? | Default value             | Description                                                                                         |
| ----------------------------------------- | --------- | ------------------------- | --------------------------------------------------------------------------------------------------- |
| `ALERT_DUPLICATE_RATIO_THRESHOLD`         | ❌ No     | `0.97`                    | The expected ratio of duplicates. If the ratio falls below this, an error is logged.                |
| `ALERT_ON_DUPLICATE`                      | ❌ No     | `false`                   | If enabled, logs an entry for every single duplicate (extremely verbose, use only for debugging).   |
| `ALERT_ON_RATIO_THRESHOLD`                | ❌ No     | `true`                    | Enables/disables alerts based on the duplicate ratio.                                               |
| `ALERT_POLL_INTERVAL`                     | ❌ No     | `1 minutes`               | Frequency of logging deduplication statistics.                                                      |
| `CACHE_SIZE`                              | ❌ No     | `2500000`                 | The maximum number of message hashes to keep in memory. Protects against OOM during traffic spikes. |
| `CACHE_TTL`                               | ❌ No     | `20 minutes`              | The time window for deduplication. Messages seen again after this period will be treated as new.    |
| `PULSAR_SERVICE_URL`                      | ✅ Yes    | `pulsar://localhost:6650` | The service URL for the Pulsar cluster.                                                             |
| `PULSAR_CONSUMER_ENABLE_MULTIPLE_TOPICS`  | ❌ No     | `false`                   | If set to `true`, the consumer will look for a topic pattern instead of a single topic.             |
| `PULSAR_CONSUMER_MULTIPLE_TOPICS_PATTERN` | ❌ No     |                           | Regex pattern for input topics (required if `PULSAR_CONSUMER_ENABLE_MULTIPLE_TOPICS` is true).      |
| `PULSAR_CONSUMER_SUBSCRIPTION`            | ❌ No     | `hfp-dedup-subscription`  | The subscription name used by the deduplicator.                                                     |
| `PULSAR_CONSUMER_TOPIC`                   | ❌ No     | `hfp-data`                | The topic to consume raw messages from.                                                             |
| `PULSAR_PRODUCER_TOPIC`                   | ❌ No     | `hfp-dedup-data`          | The topic where unique messages are published.                                                      |
