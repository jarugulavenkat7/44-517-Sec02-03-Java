 # java-word-count-beam
## A sample wordcount example pipeline using apache beam Java SDK

### Check your java verion using
java --version

### check maven verion with below command, if not present install it
maven --version

### Generate a maven project using following command
mvn archetype:generate `
 -D archetypeGroupId=org.apache.beam `
 -D archetypeArtifactId=beam-sdks-java-maven-archetypes-examples `
 -D archetypeVersion=2.36.0 `
 -D groupId=org.example `
 -D artifactId=word-count-beam `
 -D version="0.1" `
 -D package=org.apache.beam.examples `
 -D interactiveMode=false

 ### Run word count using maven 
 mvn compile exec:java -D exec.mainClass=org.apache.beam.examples.WordCount `
 -D exec.args="--inputFile=sample.txt --output=counts" -P direct-runner


## Sample Text

Created sample.txt file, Used the shakespeare's connect.

## Execution

mvn compile exec:java -D exec.mainClass=edu.nwmsu.scetion02group03.tata.MinimalPageRankTata

## Link for Project

(https://github.com/jarugulavenkat7/44-517-Sec02-03-Java)

## Link for Issues

(https://github.com/jarugulavenkat7/44-517-Sec02-03-Java/issues)

## Link for Commits

(https://github.com/jarugulavenkat7/44-517-Sec02-03-Java/commits/main)

## Link for Folder

(https://github.com/jarugulavenkat7/44-517-Sec02-03-Java/tree/main/ChandraBhanuTata)

## Link for MinimalPageRankTata

https://github.com/jarugulavenkat7/44-517-Sec02-03-Java/blob/main/ChandraBhanuTata/java-WordCount-Beam/word-count-beam/src/main/java/org/apache/beam/examples/MinimalPageRankTata.java

## Link for Wiki 
https://github.com/jarugulavenkat7/44-517-Sec02-03-Java/wiki/TATA-CHANDRA-BHANU

