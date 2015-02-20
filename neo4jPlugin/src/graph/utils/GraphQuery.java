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

package graph.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

public class GraphQuery {
	private GraphDatabaseService graphDb;

	public GraphQuery(GraphDatabaseService graphDb) {
		this.graphDb = graphDb;
	}

	public List<Long> getNeighborsAndSelf(long node_id) {
		List<Long> ls = new ArrayList<Long>();
		try (Transaction tx = graphDb.beginTx()) {
			Node n = graphDb.getNodeById(node_id);
			Iterable<Relationship> rels = n.getRelationships();

			for (Relationship rel : rels) {
				ls.add(rel.getOtherNode(n).getId());
			}

			tx.success();
		}

		ls.add(node_id);
		return ls;
	}

	public List<Long> getSubtree(long node_id) {
		List<Long> ls = new ArrayList<Long>();

		String query = "match (n)-[r:`has child`*0..]->m where id(n) = "
				+ node_id + " return id(m);";
		ExecutionEngine engine = new ExecutionEngine(graphDb);

		ExecutionResult result;
		try (Transaction tx = graphDb.beginTx()) {
			result = engine.execute(query);

			Iterator<Long> id_column = result.columnAs("id(m)");
			while (id_column.hasNext()) {
				ls.add(id_column.next());
			}

			tx.success();
		}

		return ls;
	}

	public List<Long> getSiteNode() {
		List<Long> ls = new ArrayList<Long>();

		String query = "MATCH (n:`Site`) RETURN id(n)";
		ExecutionEngine engine = new ExecutionEngine(graphDb);

		ExecutionResult result;
		try (Transaction tx = graphDb.beginTx()) {
			result = engine.execute(query);

			Iterator<Long> id_column = result.columnAs("id(n)");
			while (id_column.hasNext()) {
				ls.add(id_column.next());
			}

			tx.success();
		}

		return ls;
	}

	/*
	 * Get the info of given nodes (ids)
	 * 
	 * input: array of node id
	 * output: Map key: attribute name; Map value: array of attribute values,
	 * each attribute value (at index i)
	 * is from the node that has the given id in the input array (at index i).
	 */
	public Map<String, String[]> getNodesInfo(long[] ids) {
		Map<String, String[]> attributeMap = new HashMap<String, String[]>();
		String[] nodeID = new String[ids.length];
		attributeMap.put("neo4j_id", nodeID);

		try (Transaction tx = graphDb.beginTx()) {
			for (int i = 0; i < ids.length; i++) {
				Node n = graphDb.getNodeById(ids[i]);
				nodeID[i] = String.valueOf(ids[i]);

				for (String attributeKey : n.getPropertyKeys()) {
					if (!attributeMap.containsKey(attributeKey)) {
						String[] values = new String[ids.length];
						attributeMap.put(attributeKey, values);
					}

					attributeMap.get(attributeKey)[i] = n.getProperty(
							attributeKey).toString();
				}
			}

			tx.success();
		}

		return attributeMap;
	}

	public static void main(String[] args) {
		 List<Long> rl = (new
		 GraphQuery(DatabaseService.getDatabaseService()))
		 .getSiteNode();

		for (long n : rl) {
			System.out.print(n + ", ");
		}
		
		System.out.print("\n");
	}
}
