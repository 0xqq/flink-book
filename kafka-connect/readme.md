You can go into the github pages of a specific connector, and download it

The documentation will tell you how to configure the connector.

In this case we extracted the release tar into the kafka-connect-twitter directory.

Inside the kafka bin folder there will be a connect-standalone.sh script

The connect-standalone.properties file was taken from the config directory inside the kafka installation. The only
thing changed from the default was the line at the bottom that points to the connectors directory.

Then you run the command

    kafka-topics.sh --zookeeper localhost:2181 --create --topic twitter_status_connect --partitions 3 --replication-factor 1

This is to create the topic you'll be feeding tweets into. Then another topic:

    kafka-topics.sh --zookeeper localhost:2181 --create --topic twitter_deletes_connect --partitions 3 --replication-factor 1

You might want to test it with a consumer:

    kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic twitter_status_connect --from-beginning

Then got into your project directory:

    cd <project-directory>

and run the shell script