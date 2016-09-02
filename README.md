Kinesis Stream App
==========================

Description
-------
This application connects to the Amazon kinesis stream. For each received message, its content is displayed on the console

Configuration
-------------
The configuration file `config.properties` contains :
* Unique application name
* Number of records to receive to make a checkpoint
* Maximum number of records returned by one call
* Stream name
* Region
 
Running
--------------------

Set your AWS credentials as explained in the tutorial 
 
The configuration file `config.properties` has to be adapted.

A log file `KinesisStreamApp.log` will be created.
