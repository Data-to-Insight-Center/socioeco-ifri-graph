graphBuilder
======

A data transfer tool that can automatically export the relational IFRI dataset from MySQL into a SES graph model inside Neo4j. It leverages our concept of “Logical Object” (Jensen and Plale 2012) and is fully configurable through user-friendly CSV files. On the resulting IFRI graph model, we can perform various kinds of useful query explorations: e.g., we can find out where and why a certain species has been found disappeared. 

User Guide
==========

Software Dependencies
---------------------

To run the scripts, you need to have Python and Neo4j server installed.

1. Python: https://www.python.org/

Required Python modules:

1) py2neo: http://py2neo.org/2.0/

2) MySQLdb: http://mysql-python.sourceforge.net/

2. Neo4j: http://neo4j.com/

Create an Empty Neo4j Database
-------------------------------

Note that before you run the scripts to create the IFRI graph database, you need to change 'org.neo4j.server.database.location' in 'conf/neo4j-server.properties' to point to the new path and start this empty database with command: 'bin/neo4j start'

Configure Script Properties
---------------------------

There are properties inside the Python scripts you need to configure before executing them. Thoese properties specifiy the database connection, the CSV file format and the root SQL query for creating 'Logical Objects'.


Executing Scripts
------------------

There are two scripts inside the folder, you can run them in the same order as specified below:

1. buildNeo4jGraphFromMysql.py

Python program to convert the IFRI mysql database into a neo4j graph database using the 'Logical Object Model' specified in file 'Objects.csv' and 'Elements.csv'

The mysql database url is specified in a json data structure 'mysql_config', you can open 'buildNeo4jGraphFromMysql.py' with any text editor and change the url to the localtion of your IFRI database.

This program reads 'Objects.csv' and 'Elements.csv', so make sure they are in the same directory.

2. createSESfromCSV.py

Python program that introduce the SES framework (including the concepts and the 'parent-child' relationships read from the file 'ses_class.csv') into the neo4j graph database.


Example Graph Queries
----------------------

The file 'usefulCypherQueries.txt' has some example Cypher queries you could run on the neo4j database.

