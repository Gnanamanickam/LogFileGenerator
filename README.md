# Gnanamanickam Arumugaperumal

# Overview

The project creates a distributed map-reduce program for parallel processing of the randomly generated log messages to get insights about the log levels based on different characteristics . 
The problem is broken down into smaller tasks which can be executed parallelly using Map-Reduce in Apache Hadoop. 
The task is executed in Amazon EMR which acts as a elastic and low cost solution to quickly execute the program.

#Prerequisites

* Install SBT to build the jar
* Install VM Workstation Pro and run Horton Sandbox which comes with Apache Hadoop installation.
* AWS account to execute the jar file in Amazon EMR .

## Installation

* Clone the GIT repository by using git clone https://github.com/Gnanamanickam/LogFileGenerator.git
* Run the following commands in the console

```
sbt clean compile test
```
```
sbt clean compile run
```
* It builds and compiles the project
* If you are using IntellIj clone the repository by using "Check out from Version Control and then Git."

* The scala version should be set in the Global Libraries under Project Structure in Files .
* The SBT configuration should be added by using Edit Configurations and then simulations can be ran in the IDE .
* Now Build the project and then run the  mapReduce in hadoop to get the output. 