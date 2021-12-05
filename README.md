# spark-pull-from-kafka
Consuming Warning/Error Logs from Kafka and notifying users via Amazon Simple Email Service.
---
## Group Members
1. Karan Malhotra
2. Shahrukh Haider
3. Shashwath Jawaharlal Sathyanarayan
---

### Installations
+ Install [Simple Build Toolkit (SBT)](https://www.scala-sbt.org/1.x/docs/index.html)
+ Ensure you can create, compile and run Java and Scala programs.

### Development Environment
+ Windows 10
+ Java 11.0.11
+ Scala 2.12.19
+ SBT Script Version 1.5.5
+ Other dependencies exist in build.sbt
+ IntelliJ IDEA Ultimate
+ Apache Spark Version: 
+ Kafka Version: 
+ AWS Simple Email Service

## Project Overview
+ In this project a streaming data pipeline was created by designing and implementing an actor-model service using Akka.
+ This model  ingests logfile generated data in real time and delivers it via an event-based service called Kafka to Spark for further processing. 
+ An aggregation of the total number of ERROR and WARN messages was computed and the stakeholders were notified via email sent using AWS SES.

## Setting up Kafka
1. To set up kafka in the cloud we will be making use of Amazon's Managed Streaming for Apache Kafka. Amazon MSK is a fully managed service that enables one to build and run applications that use Apache Kafka to process streaming data which in our case are the log messages being passed from the Akka actor system. The following steps are to be followed to set up kafka :-
- Use the "Quickly create a cluster" option choosing a kafka.t3.small EC2 broker instance having a EBS storage of 2 GB and launch the cluster
- Next create a Linux t2.large EC2 instance and launch the instance
- SSH into the created EC2 instance and execute the following commands :
- Install Java: `sudo yum install java-1.8.0`
- Get Kafka: `wget  https://archive.apache.org/dist/kafka/2.2.1/kafka_2.12-2.2.1.tgz`
- Extract Kafka: `tar -xzvf kafka_2.12-2.2.1.tgz`
- Get Cluster ARN: `aws kafka describe-cluster --cluster-arn "ClusterArn" --region region`
- Create Topic: `bin/kafka-topics.sh --create --zookeeper "ZookeeperConnectString" --replication-factor 2 --partitions 1 --topic topic`
2. Now we have to produce and consume data from the above crested kafka stream:
- Get the name of the Java JVM from the java runtime and use the Java trust store command which is a copy command to fetch the trust door from Java: `cp /usr/lib/jvm/"Java JVM name"/jre/lib/security/cacerts /tmp/kafka.client.truststore.jks`
3. Switch directories to kafka's bin folder and create a client properties file that contains a security protocol on the trust store location.
- client.properties: `security.protocol=SSL ssl.truststore.location=/tmp/kafka.client.truststore.jks`
4. To start creating messages we need to get the bootstrap broker string TSL 
- Get Broker's TLS string: `aws kafka get-bootstrap-brokers --cluster-arn ClusterArn --region`
5. Command to start the producer: `./kafka-console-producer.sh --broker-list "BootstrapBrokerStringTls" --producer.config client.properties --topic Topics`
6. Start another EC2 session for the consumer and use the following command to start the consumer: `./kafka-console-consumer.sh --bootstrap-server "BootstrapBrokerStringTls" --consumer.config client.properties --topic "Topics" --from-beginning!`


## Setting up Apache Spark
1. The main abstraction Spark provides is a resilient distributed dataset (RDD), which is a collection of elements partitioned across the nodes of the cluster that can be operated on in parallel
- The following Spark dependencies were added in the `build.sbt` file:
	`"org.apache.spark"%%"spark-core"%"3.0.3"`
	`"org.apache.spark"%%"spark-streaming"%"3.0.3"`
	`"org.apache.spark"%%"spark-streaming-kafka-0-10"%"3.0.3"`
2. The first thing a Spark program must do is to create a SparkContext object, which tells Spark how to access a cluster. To create a SparkContext one needs to build a SparkConf object that contains information about the application
3. Then a Kafka consumer to fetch error and warn log messages via the spark streams was implemented 
4. Finally every line received from consumer via the spark stream was iterated over and an email was sent for the same.

### Steps to Run the Spark-Pull-From-Kafka Application
1. Install Scala
 - `wget http://downloads.lightbend.com/scala/2.11.8/scala-2.11.8.rpm`
 - `sudo yum install scala-2.11.8.rpm`
3. Install SBT
 - `curl -L https://www.scala-sbt.org/sbt-rpm.repo > sbt-rpm.repo`
 - `sudo mv sbt-rpm.repo /etc/yum.repos.d/`
 - `sudo yum install sbt`
5. Install Git
 - `sudo yum install git`
7. Clone this repo
9. Execute the Spark Program by typing `sbt "runMain KafkaSparkIntegration"` in the root directory of this project.

## Test Cases
1. `testCheckConfig` tests if Configs exists or not.
2. `testBrokerTLS` confirms whether the broker is empty.
3. `testFROMEmailIDInstance` confirms the email address is of type InternetAddress.
4. `testRecipientEmailAddresses` confirms that the list of email address set as recipients is of type Array[InternetAddress].
5. `testJKSFileExists` sees if the JKS file `kafka.client.truststore.jks` is inside the directory as mentioned in the instructions above as well.

## Output
<p align="center">
  <img src="https://github.com/karanmalh0tra/spark-pull-from-kafka/blob/main/Images/output.png" alt="output image"/>
</p>

## Other Repos
+ https://github.com/shaide32/LogGenerator
+ https://github.com/shaide32/s3-file-watcher

Video Link:
