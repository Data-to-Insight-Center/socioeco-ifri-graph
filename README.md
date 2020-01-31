### Disclaimer 
<b>This code repository is no longer being actively managed by the <a target="_blank" rel="noopener noreferrer" href="https://pti.iu.edu/centers/d2i/">Data To Insight Center</a> at Indiana University Bloomington. Neither the center nor its principals assume responsibility for vulnerabilities that the code may have acquired over time.</b>

socioeco-ifri-graph
======

Socio-Eco tools for the graph modeling and analysis of IFRI data:

1. A data transfer tool that can automatically export the relational IFRI dataset from MySQL into a SES graph model inside Neo4j. It leverages our concept of “Logical Object” (Jensen and Plale 2012) and is fully configurable through user-friendly CSV files. On the resulting IFRI graph model, we can perform various kinds of useful query explorations: e.g., we can find out where and why a certain species has been found disappeared. 

2. A neo4j extension to provide the advanced data mining features (node clustering and the sub-graph matching) on the SES graph model. The feature of node clustering indicates which nodes are similar to each other based on their attribute values, and can reveal the outliers (if exist) that could be particularly interesting. The feature of sub-graph matching is extended from our previous work on provenance graph matching (Chen and Plale 2012). It can be used to find out for any two site visits on the same location but of different dates, what have been changed and what remain unchanged.

