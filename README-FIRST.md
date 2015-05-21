# Temperature-Analysis
a beginner experiments with map reduce to find the min,max and sorting of temperatures from a census data set.

I have tried and tested these programs in the following environment
	Operating system - Mac OS X 10.10.3  Mac book pro with 8GB memory
         Java version - 1.7
         Horton works sandbox running Hadoop 2.6.0
         Eclipse luna used as IDE with Hadoop 2.6 jars added externally.

Some pointers which I have learned through my experimentaton
1.  While exporting to a jar in eclipse , choose the option runnable jar 
2.  Try to the make the program work on a very smaller sample data first , before moving on to the bigger data sets
3.  Set the jar permission to executable in the sandbox(it runs a flavour of centOS) some times it may not be happy otherwise.
4.  hadoop jar <jar name> <input file path in hdfs> < ouput file path in hdfs> is format for running the jar on the sandbox
5.  There is a web interface for horton works where in users can view,download and upload files to hdfs (simplest way :) )
