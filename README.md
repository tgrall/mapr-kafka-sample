# Sample Application to send XML into MapR Cluster

* Producer generates XML files
* Consumer save it into file, as XML or JSON (for example MapR-FS loaded using NFS)

Note: this is a quick & dirty application created for demo purpose.

### How to run the demo

* Start the Kafka cluster
* Create a TR069 topic
* Create proper log files (XML and JSON - this needs to be sent as parameter)
* Launch the producer
* Launch the consumer that append XML to log
* Launch the consumer that append JSON to log (use Drill on this file)

As mention before : quick & dirty, so not safe (bad IO operations)