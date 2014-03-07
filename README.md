Spark on Cassandra QuickStart
===========================

This project is meant to help people get up and running quickly with
Spark and Cassandra.

To get going, simply create the schema in a local Cassandra host.

	        cat schema/northpole.cql | ~/tools/cassandra/bin/cqlsh

Then run maven:

		mvn clean install

The code is all in a single file:

		./src/test/scala/com/github/boneill42/FindChildrenTest.scala

Happy Sparking.


