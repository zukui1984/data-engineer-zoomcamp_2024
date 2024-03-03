# Apache Spark Guide

## Introduction

Briefly introduce Apache Spark, emphasizing its importance in big data processing and analytics.

## Prerequisites

List the prerequisites for working with Apache Spark, including software requirements and basic knowledge foundations.

## Installation

### MacOS

```bash
# Install Java
brew cask install java

# Install Scala
brew install scala

# Install Apache Spark
brew install apache-spark
```

## Setting Up Java and Spark on Linux/Bash
### Java installation
1. Download Java 11 using wget:
wget [download link>](https://jdk.java.net/archive/) 
2. Unzip the downloaded file:
```bash
tar xzvf openjdk-11.0.2_linux-x64_bin.tar.gz
```
3. Remove the compressed file:
```bash
rm openjdk-11.0.2_linux-x64_bin.tar.gz
```
4. Set the Java home path and update your system's PATH variable:
```bash
export JAVA_HOME="/mnt/c/Users/Anwender/Desktop/spark/jdk-11.0.2"
export PATH="${JAVA_HOME}/bin:${PATH}"
```
## Spark installation
1. Download Spark 3.3.2:
```bash
wget https://archive.apache.org/dist/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz
```
2. Unpack the Spark archive and remove the compressed file:
```bash
tar xzfv spark-3.3.2-bin-hadoop3.tgz
rm spark-3.3.2-bin-hadoop3.tgz
```
3. Set the Spark home path and update the PATH variable:
```bash
export SPARK_HOME="/mnt/c/Users/Anwender/Desktop/spark/spark-3.3.2-bin-hadoop3"
export PATH="${SPARK_HOME}/bin:${PATH}"
```
## Running Spark
1. Launch Spark shell:
```bash
spark-shell
```
2. Test with Scala commands:
```bash
val data = 1 to 10000
val distData = sc.parallelize(data)
distData.filter(_ < 10).collect()
```
## Persistent environment setup
1. Add Java and Spark paths to ~/.bashrc for persistence:
```bash
echo 'export JAVA_HOME="/mnt/c/Users/Anwender/Desktop/spark/jdk-11.0.2"' 
echo 'export PATH="$JAVA_HOME/bin:$PATH"' 
echo 'export SPARK_HOME="/mnt/c/Users/Anwender/Desktop/spark/spark-3.3.2-bin-hadoop3"' 
echo 'export PATH="$SPARK_HOME/bin:$PATH"'
add all above into nano ~/.bashrc
```



# Install Apache Spark
```
Download Spark from the [official website](https://spark.apache.org) and follow the installation instructions.

### Spark fundamentals
```scala
// Example: Creating a SparkSession
val spark = SparkSession.builder()
  .appName("Spark Example")
  .config("spark.some.config.option", "some-value")
  .getOrCreate()
```
### Working with DataFrames
```scala
// Example: Reading a JSON file into a DataFrame
val df = spark.read.json("path/to/json/file")
df.show()
```
### SQL Operations
```scala
// Register the DataFrame as a SQL temporary view
df.createOrReplaceTempView("people")

// Perform a SQL query
val sqlDF = spark.sql("SELECT * FROM people")
sqlDF.show()
```
### Custom schema
```scala
// Define a custom schema
val schema = new StructType()
  .add("name", StringType)
  .add("age", IntegerType)

// Apply the schema to the read operation
val peopleDF = spark.read.schema(schema).json("path/to/json/file")
peopleDF.show()
```
