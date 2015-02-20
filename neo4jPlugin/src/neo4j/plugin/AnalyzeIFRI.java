/*
#
# Copyright 2015 The Trustees of Indiana University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
*/

package neo4j.plugin;

import graph.clustering.NodeClusterer;
import graph.matching.SubGraphMatching;
import graph.utils.GraphQuery;

import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.server.plugins.Description;
import org.neo4j.server.plugins.Name;
import org.neo4j.server.plugins.Parameter;
import org.neo4j.server.plugins.PluginTarget;
import org.neo4j.server.plugins.ServerPlugin;
import org.neo4j.server.plugins.Source;

@Description("An extension to the Neo4j Server for analyzing the IFRI data")
public class AnalyzeIFRI extends ServerPlugin {
	@Name("cluster_siteNodes")
	@Description("Perform clustering on all site nodes in the Neo4j graph database")
	@PluginTarget(GraphDatabaseService.class)
	public String clusterSiteNodes(
			@Source GraphDatabaseService graphDb,
			@Description("How many clusters to create") 
			@Parameter(name = "numOfClusters") int numberOfClusters,
			@Description("Whether to save clustering assignments to nodes or not, default value is false.") 
			@Parameter(name = "saveResult", optional = true) Boolean saveResult) {
		List<Long> nodeIds = (new GraphQuery(graphDb)).getSiteNode();
		long[] ids = new long[nodeIds.size()];
		for (int i = 0; i < ids.length; i++) {
			ids[i] = nodeIds.get(i);
		}
		
		System.out.println("clusterSiteNodes: " + ids.length 
				+ " site nodes to cluster");

		NodeClusterer clusterer = new NodeClusterer(graphDb);
		saveResult = (saveResult == null) ? false : saveResult;
		int[] assignments = clusterer.run(ids, numberOfClusters, saveResult);

		// write the result into a json string
		StringBuffer sb = new StringBuffer();
		sb.append("{\"nodeIds\": [");
		for (int i = 0; i < ids.length; i++) {
			if (i > 0) {
				sb.append(", ");
			}
			sb.append(ids[i]);
		}
		sb.append("]");

		sb.append(", \"assignments\": [");
		for (int i = 0; i < assignments.length; i++) {
			if (i > 0) {
				sb.append(", ");
			}
			sb.append(assignments[i]);
		}
		sb.append("]}");

		return sb.toString();
	}

	@Name("cluster_nodes")
	@Description("Perform clustering on the given nodes from the Neo4j graph database")
	@PluginTarget(GraphDatabaseService.class)
	public String clusterNodes(
			@Source GraphDatabaseService graphDb,
			@Description("Id of nodes to be clustered") 
			@Parameter(name = "nodeIds") long[] nodeIds,
			@Description("How many clusters to create") 
			@Parameter(name = "numOfClusters") int numberOfClusters,
			@Description("Whether to save clustering assignments to nodes or not, default value is false.") 
			@Parameter(name = "saveResult", optional = true) Boolean saveResult) {
		System.out.println("clusterNodes: " + nodeIds.length 
				+ " nodes to cluster");
		
		NodeClusterer clusterer = new NodeClusterer(graphDb);
		saveResult = (saveResult == null) ? false : saveResult;
		int[] assignments = clusterer
				.run(nodeIds, numberOfClusters, saveResult);

		// write the result into a json string
		StringBuffer sb = new StringBuffer();
		sb.append("{\"nodeIds\": [");
		for (int i = 0; i < nodeIds.length; i++) {
			if (i > 0) {
				sb.append(", ");
			}
			sb.append(nodeIds[i]);
		}
		sb.append("]");

		sb.append(", \"assignments\": [");
		for (int i = 0; i < assignments.length; i++) {
			if (i > 0) {
				sb.append(", ");
			}
			sb.append(assignments[i]);
		}
		sb.append("]}");

		return sb.toString();
	}
	
	@Name("graph_matching")
	@Description("Perform graph matching algorithm on two subgraphs from the Neo4j graph database")
	@PluginTarget(GraphDatabaseService.class)
	public String graphMatching(
			@Source GraphDatabaseService graphDb,
			@Description("Ids of first subgraph") 
			@Parameter(name = "nodeIdsA") long[] nodeIdsA,
			@Description("Ids of second subgraph") 
			@Parameter(name = "nodeIdsB") long[] nodeIdsB,
			@Description("Whether to save matching assignments to nodes or not, default value is false.") 
			@Parameter(name = "saveResult", optional = true) Boolean saveResult) {

		System.out.println("graphMatching: " + nodeIdsA.length 
				+ " nodes in the first graph, " + nodeIdsB.length 
				+ " nodes in the second graph");

		Map<Integer, Map<Long, Long>> results = (new SubGraphMatching(graphDb))
				.performMatching(nodeIdsA, nodeIdsB, saveResult);

		// write the result into a json string
		StringBuffer sb = new StringBuffer();
		
		sb.append("{\"subgraphs\" : ");
		
		for (Integer subGraphId : results.keySet()) {
			sb.append("{\"subgraphId\" : " + subGraphId);
			
			Map<Long, Long> subgraphAssignments = results.get(subGraphId);
			Long[] nodesA = new Long[subgraphAssignments.keySet().size()];
			nodesA = subgraphAssignments.keySet().toArray(nodesA);
			
			sb.append(", \"nodeIds\": [");
			for (int i=0; i<nodesA.length; i++) {
				if (i > 0) {
					sb.append(", ");
				}
				sb.append(nodesA[i]);
			}
			sb.append("]");
			
			sb.append(", \"assignments\": [");
			for (int i=0; i<nodesA.length; i++) {
				if (i > 0) {
					sb.append(", ");
				}
				sb.append(subgraphAssignments.get(nodesA[i]));
			}
			sb.append("]}");
		}
		sb.append("}");

		return sb.toString();
	}

	@Name("site_matching")
	@Description("Perform graph matching algorithm on the subtrees of two site nodes from the Neo4j graph database")
	@PluginTarget(GraphDatabaseService.class)
	public String siteMatching(
			@Source GraphDatabaseService graphDb,
			@Description("Id of first site node") 
			@Parameter(name = "siteNodeA") long siteNodeA,
			@Description("Id of second site node") 
			@Parameter(name = "siteNodeB") long siteNodeB,
			@Description("Whether to save matching assignments to nodes or not, default value is false.") 
			@Parameter(name = "saveResult", optional = true) Boolean saveResult) {

		GraphQuery query = new GraphQuery(graphDb);

		List<Long> r1 = query.getSubtree(siteNodeA);
		long[] nodes_1 = new long[r1.size()];
		for (int i = 0; i < nodes_1.length; i++) {
			nodes_1[i] = r1.get(i);
		}

		List<Long> r2 = query.getSubtree(siteNodeB);
		long[] nodes_2 = new long[r2.size()];
		for (int i = 0; i < nodes_2.length; i++) {
			nodes_2[i] = r2.get(i);
		}

		System.out.println("siteMatching: " + nodes_1.length 
				+ " site nodes in the first graph, " + nodes_2.length 
				+ " site nodes in the second graph");
		
		Map<Integer, Map<Long, Long>> results = (new SubGraphMatching(graphDb))
				.performMatching(nodes_1, nodes_2, saveResult);

		// write the result into a json string
		StringBuffer sb = new StringBuffer();
		
		sb.append("{\"subgraphs\" : ");
		
		for (Integer subGraphId : results.keySet()) {
			sb.append("{\"subgraphId\" : " + subGraphId);
			
			Map<Long, Long> subgraphAssignments = results.get(subGraphId);
			Long[] nodesA = new Long[subgraphAssignments.keySet().size()];
			nodesA = subgraphAssignments.keySet().toArray(nodesA);
			
			sb.append(", \"nodeIds\": [");
			for (int i=0; i<nodesA.length; i++) {
				if (i > 0) {
					sb.append(", ");
				}
				sb.append(nodesA[i]);
			}
			sb.append("]");
			
			sb.append(", \"assignments\": [");
			for (int i=0; i<nodesA.length; i++) {
				if (i > 0) {
					sb.append(", ");
				}
				sb.append(subgraphAssignments.get(nodesA[i]));
			}
			sb.append("]}");
		}
		sb.append("}");

		return sb.toString();
	}
}
