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

package graph.matching;

import graph.utils.DatabaseService;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

public class SubGraphMatching {
	public static double THRESHOLD = 0.6;
	private GraphDatabaseService graphDb;

	public SubGraphMatching(GraphDatabaseService graphDb) {
		this.graphDb = graphDb;
	}

	/*
	 * @param nodes_1 -- array of the node ids from the first graph
	 * 
	 * @param nodes_2 -- array of the node ids from the second graph
	 * 
	 * @param saveResult -- whether to save the matching assignments back to
	 * nodes or
	 * not
	 * 
	 * @output -- Mapping results to be returned <key: subgraphIndex, value:
	 * mappings>
	 */
	public Map<Integer, Map<Long, Long>> performMatching(long[] nodes_1,
			long[] nodes_2, boolean saveResult) {
		// comparing against larger set of nodes
		long[] nodes_lg, nodes_sm;
		// CyNetwork network_lg, network_sm;

		if (nodes_1.length > nodes_2.length) {
			nodes_sm = nodes_2;
			nodes_lg = nodes_1;
		} else {
			nodes_sm = nodes_1;
			nodes_lg = nodes_2;
		}

		Map<Long, Integer> id_to_index_map_sm = new HashMap<Long, Integer>();
		Map<Long, Integer> id_to_index_map_lg = new HashMap<Long, Integer>();

		for (int i = 0; i < nodes_sm.length; i++) {
			id_to_index_map_sm.put(nodes_sm[i], i);
		}
		for (int i = 0; i < nodes_lg.length; i++) {
			id_to_index_map_lg.put(nodes_lg[i], i);
		}

		/*
		 * Set the sub-graph number, and clear the node matching records
		 */
		try (Transaction tx = graphDb.beginTx()) {

			for (long node_id : nodes_sm) {
				Node n = graphDb.getNodeById(node_id);
				if (n.hasProperty("sub-graph-match")) {
					n.removeProperty("sub-graph-match");
				}
				if (n.hasProperty("matching-node-id")) {
					n.removeProperty("matching-node-id");
				}
			}
			for (long node_id : nodes_lg) {
				Node n = graphDb.getNodeById(node_id);
				if (n.hasProperty("sub-graph-match")) {
					n.removeProperty("sub-graph-match");
				}
				if (n.hasProperty("matching-node-id")) {
					n.removeProperty("matching-node-id");
				}
			}

			tx.success();
		}

		/* create adjacency matrix */
		int[][] adjacencyMatrix_sm = new int[nodes_sm.length][nodes_sm.length];
		int[][] adjacencyMatrix_lg = new int[nodes_lg.length][nodes_lg.length];

		try (Transaction tx = graphDb.beginTx()) {

			for (int i = 0; i < nodes_sm.length; i++) {
				Node n = graphDb.getNodeById(nodes_sm[i]);
				Iterable<Relationship> relations = n
						.getRelationships(Direction.OUTGOING);

				for (Relationship rel : relations) {
					long endNodeId = rel.getEndNode().getId();
					if (id_to_index_map_sm.containsKey(endNodeId)) {
						adjacencyMatrix_sm[i][id_to_index_map_sm.get(endNodeId)] = 1;
					}
				}
			}

			for (int i = 0; i < nodes_lg.length; i++) {
				Node n = graphDb.getNodeById(nodes_lg[i]);
				Iterable<Relationship> relations = n
						.getRelationships(Direction.OUTGOING);

				for (Relationship rel : relations) {
					long endNodeId = rel.getEndNode().getId();
					// there could be nodes not in the subgraph to match
					if (id_to_index_map_lg.containsKey(endNodeId)) {
						adjacencyMatrix_lg[i][id_to_index_map_lg.get(endNodeId)] = 1;
					}
				}
			}

			tx.success();
		}

		/* create layers of connectivity matrix */
		int N = nodes_lg.length;
		int Vmax = (int) Math.ceil(N / 4.0);

		int[][][] connectivityMatrixs_sm = new int[Vmax][nodes_sm.length][nodes_sm.length];
		int[][][] connectivityMatrixs_lg = new int[Vmax][nodes_lg.length][nodes_lg.length];

		connectivityMatrixs_sm[0] = adjacencyMatrix_sm;
		connectivityMatrixs_lg[0] = adjacencyMatrix_lg;
		for (int i = 1; i < Vmax; i++) {
			connectivityMatrixs_sm[i] = multiply(connectivityMatrixs_sm[i - 1],
					connectivityMatrixs_sm[i - 1]);
			connectivityMatrixs_lg[i] = multiply(connectivityMatrixs_lg[i - 1],
					connectivityMatrixs_lg[i - 1]);
		}

		// Compare each pair of nodes in each graph
		double[][] attendanceRating = new double[nodes_sm.length][nodes_lg.length];

		for (int i = 0; i < nodes_sm.length; i++) {
			for (int j = 0; j < nodes_lg.length; j++) {
				// if they have the same node id (refer to the same neo4j node),
				// we decide to use them as the 'anchor points'
				if (nodes_sm[i] == nodes_lg[j]) {
					System.out.println("anchor point: " + nodes_sm[i]);
					attendanceRating[i][j] = 1;
					continue;
				}

				// compare incoming edges of current nodes
				double[] in_bestMatchingEdgeSimilarity = calculateEdgeSimilarity(
						nodes_sm[i], nodes_lg[j], connectivityMatrixs_sm,
						connectivityMatrixs_lg, id_to_index_map_sm,
						id_to_index_map_lg, Direction.INCOMING);

				// compare outgoing edges of current nodes
				double[] out_bestMatchingEdgeSimilarity = calculateEdgeSimilarity(
						nodes_sm[i], nodes_lg[j], connectivityMatrixs_sm,
						connectivityMatrixs_lg, id_to_index_map_sm,
						id_to_index_map_lg, Direction.OUTGOING);

				/* Compare current nodes */
				/* No need to Compare connectivity */

				/* Compare node properties */
				double similarity_node = calculateNodeSimilarity(nodes_sm[i],
						nodes_lg[j]);

				/*
				 * Find similarity of current nodes Combine (3a) (2a) and
				 * (2b)
				 * to form attendance rating, using TE
				 */
				similarity_node = 1 - similarity_node;
				for (int k = 0; k < in_bestMatchingEdgeSimilarity.length; k++) {
					// similarity_node *= Math.pow(
					// (1 - in_bestMatchingEdgeSimilarity[k]), 10);
					similarity_node *= 1 - in_bestMatchingEdgeSimilarity[k];
				}

				for (int k = 0; k < out_bestMatchingEdgeSimilarity.length; k++) {
					// similarity_node *= Math.pow(
					// (1 - out_bestMatchingEdgeSimilarity[k]), 10);
					similarity_node *= 1 - out_bestMatchingEdgeSimilarity[k];
				}

				attendanceRating[i][j] = 1 - similarity_node;
		}

		/*
		 * Form matching subgraphs 12 5) Find peaks of attendance ratings to
		 * define
		 * mapping from all n to n ik 12 12
		 * 
		 * 6) Form subgraphs g and g using node pairs n and n with attendance
		 * ratings above threshold T ik 1 1212
		 * 
		 * 7) Use node-to-node mapping to reorder nodes in g such that n maps to
		 * n1, n maps to n , etc
		 */

		// Mapping results to be returned
		// <key: subgraphIndex, value: mappings>
		Map<Integer, Map<Long, Long>> results = new HashMap<Integer, Map<Long, Long>>();

		// keep track of already matched nodes
		Set<Long> total_sm_matched_nodes = new HashSet<Long>();
		Set<Long> total_lg_matched_nodes = new HashSet<Long>();
		int subgraphIndex = 1;

		while (true) {
			// find the peak attendance node's pair
			double peak = 0;
			int index_1 = 0, index_2 = 0;
			for (int i = 0; i < attendanceRating.length; i++) {
				if (!total_sm_matched_nodes.contains(nodes_sm[i]))
					for (int j = 0; j < attendanceRating[i].length; j++) {
						if (!total_lg_matched_nodes.contains(nodes_lg[j])
								&& attendanceRating[i][j] > peak) {
							peak = attendanceRating[i][j];
							index_1 = i;
							index_2 = j;
						}
					}
			}

			System.out.println("peak attendance:" + peak);
			Map<Long, Long> current_matched_nodes_pair = new HashMap<Long, Long>();
			if (peak > THRESHOLD) {
				System.out.println("matched:" + nodes_sm[index_1] + "->"
						+ nodes_lg[index_2] + " at attendance: " + peak);
				current_matched_nodes_pair.put(nodes_sm[index_1],
						nodes_lg[index_2]);
				total_sm_matched_nodes.add(nodes_sm[index_1]);
				total_lg_matched_nodes.add(nodes_lg[index_2]);
			} else
				break;

			// go through all the neighbors of each node, extend the subgraph
			// with
			// the largest attendance
			extendFromCurrentMatches(current_matched_nodes_pair,
					total_sm_matched_nodes, total_lg_matched_nodes,
					id_to_index_map_sm, id_to_index_map_lg, attendanceRating);

			/*
			 * store the matching results
			 */
			if (saveResult) {
				storeMatchingResults(current_matched_nodes_pair, subgraphIndex);
			}

			results.put(subgraphIndex, current_matched_nodes_pair);
			subgraphIndex++;
		}

		return results;
	}

	private double[] calculateEdgeSimilarity(long node_1, long node_2,
			int[][][] connectivityMatrixs_1, int[][][] connectivityMatrixs_2,
			Map<Long, Integer> id_to_index_map_1,
			Map<Long, Integer> id_to_index_map_2, Direction direction) {

		try (Transaction tx = graphDb.beginTx()) {

			// compare incoming edges of current nodes
			Node n_1 = graphDb.getNodeById(node_1);
			Node n_2 = graphDb.getNodeById(node_2);

			// matching against the larger set of edges
			Node node_lg, node_sm;
			int[][][] connectivityMatrixs_edge_lg, connectivityMatrixs_edge_sm;
			Map<Long, Integer> id_to_index_map_sm, id_to_index_map_lg;
			if (n_1.getDegree(direction) > n_2.getDegree(direction)) {
				node_sm = n_2;
				node_lg = n_1;

				connectivityMatrixs_edge_sm = connectivityMatrixs_2;
				connectivityMatrixs_edge_lg = connectivityMatrixs_1;

				id_to_index_map_sm = id_to_index_map_2;
				id_to_index_map_lg = id_to_index_map_1;
			} else {
				node_sm = n_1;
				node_lg = n_2;

				connectivityMatrixs_edge_sm = connectivityMatrixs_1;
				connectivityMatrixs_edge_lg = connectivityMatrixs_2;

				id_to_index_map_sm = id_to_index_map_1;
				id_to_index_map_lg = id_to_index_map_2;
			}

			int toNode_index_sm = id_to_index_map_sm.get(node_sm.getId());
			int toNode_index_lg = id_to_index_map_lg.get(node_lg.getId());

			double[] bestMatchingEdgeSimilarity = new double[node_sm
					.getDegree(direction)];
			int rel_index = 0;

			for (Relationship rel_sm : node_sm.getRelationships(direction)) {
				Node fromNode_sm = rel_sm.getStartNode();
				// there could be nodes not in the subgraph to match
				if (false == id_to_index_map_sm
						.containsKey(fromNode_sm.getId())) {
					continue;
				}
				int tmp_fromNode_index_sm = id_to_index_map_sm.get(fromNode_sm
						.getId());

				for (Relationship rel_lg : node_lg.getRelationships(direction)) {
					Node fromNode_lg = rel_lg.getStartNode();
					if (false == id_to_index_map_sm.containsKey(fromNode_lg
							.getId())) {
						continue;
					}
					int tmp_fromNode_index_lg = id_to_index_map_sm
							.get(fromNode_lg.getId());

					// compare connectivity of
					// node<fromNode_index_sm,toNode_index_sm> and
					// node<fromNode_index_lg,toNode_index_lg>
					double comparedSignature = 0;
					for (int v = 0; v < connectivityMatrixs_edge_lg.length; v++) {
						double diff = connectivityMatrixs_edge_lg[v][tmp_fromNode_index_sm][toNode_index_sm]
								- connectivityMatrixs_edge_sm[v][tmp_fromNode_index_lg][toNode_index_lg];
						comparedSignature += 1.0 / (1.0 + Math.abs(diff));
					}

					comparedSignature /= connectivityMatrixs_edge_lg.length;

					// compare edge properties
					Iterable<String> edge_attr_name_sm = rel_sm
							.getPropertyKeys();
					Iterable<String> edge_attr_name_lg = rel_lg
							.getPropertyKeys();

					Set<String> edge_attr_name = new HashSet<String>();
					for (String name : edge_attr_name_sm) {
						edge_attr_name.add(name);
					}
					for (String name : edge_attr_name_lg) {
						edge_attr_name.add(name);
					}

					double similarity_edge = 0;
					for (String name : edge_attr_name) {
						if (rel_sm.hasProperty(name)
								&& rel_lg.hasProperty(name)) {
							int diff = rel_sm
									.getProperty(name)
									.toString()
									.compareTo(
											rel_lg.getProperty(name).toString());
							similarity_edge += 1.0 / (1.0 + Math.abs(diff));
						}
					}
					similarity_edge /= edge_attr_name.size();

					// compare node properties between otherNode_sm and
					// otherNode_lg
					Node otherNode_sm = rel_sm.getOtherNode(node_sm);
					Node otherNode_lg = rel_lg.getOtherNode(node_lg);

					Iterable<String> node_attr_name_sm = otherNode_sm
							.getPropertyKeys();
					Iterable<String> node_attr_name_lg = otherNode_lg
							.getPropertyKeys();

					Set<String> node_attr_name = new HashSet<String>();
					for (String name : node_attr_name_sm) {
						node_attr_name.add(name);
					}
					for (String name : node_attr_name_lg) {
						node_attr_name.add(name);
					}

					double similarity_node = 0;
					for (String name : node_attr_name) {
						if (otherNode_sm.hasProperty(name)
								&& otherNode_lg.hasProperty(name)) {
							int diff = otherNode_sm
									.getProperty(name)
									.toString()
									.compareTo(
											otherNode_lg.getProperty(name)
													.toString());
							similarity_node += 1.0 / (1.0 + Math.abs(diff));
						}
					}
					similarity_node /= node_attr_name.size();

					// combine results using IOP (Independent Opinion
					// Pole)
					double similarity = comparedSignature * similarity_edge
							* similarity_node;
					// String dir = (direction == Direction.INCOMING) ?
					// "incoming"
					// : "outgoing";
					// System.out.println(dir + "edges similarity:" + similarity
					// + "(comparedSignature:" + comparedSignature
					// + " similarity_edge:" + similarity_edge
					// + " similarity_node:" + similarity_node + ")");

					// Save comparison of best matching edges and
					// adjacent
					// nodes
					if (bestMatchingEdgeSimilarity[rel_index] < similarity)
						bestMatchingEdgeSimilarity[rel_index] = similarity;
				}
				rel_index++;
			}

			tx.success();

			return bestMatchingEdgeSimilarity;
		}
	}

	private double calculateNodeSimilarity(long node_sm_id, long node_lg_id) {
		double similarity_node = 0;
		try (Transaction tx = graphDb.beginTx()) {

			Node node_sm = graphDb.getNodeById(node_sm_id);
			Node node_lg = graphDb.getNodeById(node_lg_id);
			Iterable<String> node_attr_name_sm = node_sm.getPropertyKeys();
			Iterable<String> node_attr_name_lg = node_lg.getPropertyKeys();

			Set<String> node_attr_name = new HashSet<String>();
			for (String name : node_attr_name_sm) {
				node_attr_name.add(name);
			}
			for (String name : node_attr_name_lg) {
				node_attr_name.add(name);
			}

			for (String name : node_attr_name) {
				if (node_sm.hasProperty(name) && node_lg.hasProperty(name)) {
					int diff = node_sm.getProperty(name).toString()
							.compareTo(node_lg.getProperty(name).toString());
					similarity_node += 1.0 / (1.0 + Math.abs(diff));
				}
			}
			similarity_node /= node_attr_name.size();

			tx.success();
			return similarity_node;
		}
	}

	private void extendFromCurrentMatches(
			Map<Long, Long> current_matched_nodes_pair,
			Set<Long> total_sm_matched_nodes, Set<Long> total_lg_matched_nodes,
			Map<Long, Integer> id_to_index_map_sm,
			Map<Long, Integer> id_to_index_map_lg, double[][] attendanceRating) {
		try (Transaction tx = graphDb.beginTx()) {

			while (true) {
				Iterator<Long> itr = current_matched_nodes_pair.keySet()
						.iterator();

				double best_attendance = 0;
				long best_extension_neighbor_1 = 0, best_extension_neighbor_2 = 0;

				while (itr.hasNext()) {
					long node_1 = itr.next();
					long node_2 = current_matched_nodes_pair.get(node_1);

					Iterable<Relationship> rels_1 = graphDb.getNodeById(node_1)
							.getRelationships();
					for (Relationship rel_1 : rels_1) {
						long neighbor_1 = rel_1.getOtherNode(
								graphDb.getNodeById(node_1)).getId();
						if (false == id_to_index_map_sm.containsKey(neighbor_1)) {
							continue;
						}
						if (total_sm_matched_nodes.contains(neighbor_1))
							continue;

						int neighbor_index_1 = id_to_index_map_sm
								.get(neighbor_1);

						Iterable<Relationship> rels_2 = graphDb.getNodeById(
								node_2).getRelationships();
						for (Relationship rel_2 : rels_2) {
							long neighbor_2 = rel_2.getOtherNode(
									graphDb.getNodeById(node_2)).getId();
							if (false == id_to_index_map_lg
									.containsKey(neighbor_2)) {
								continue;
							}
							if (total_lg_matched_nodes.contains(neighbor_2))
								continue;

							int neighbor_index_2 = id_to_index_map_lg
									.get(neighbor_2);

							if (attendanceRating[neighbor_index_1][neighbor_index_2] > best_attendance) {
								best_attendance = attendanceRating[neighbor_index_1][neighbor_index_2];
								best_extension_neighbor_1 = neighbor_1;
								best_extension_neighbor_2 = neighbor_2;
							}
						}
					}

				}

				if (best_attendance > THRESHOLD) {
					System.out.println("extending matched:"
							+ best_extension_neighbor_1 + "->"
							+ best_extension_neighbor_2 + " at attendance: "
							+ best_attendance);
					current_matched_nodes_pair.put(best_extension_neighbor_1,
							best_extension_neighbor_2);
					total_sm_matched_nodes.add(best_extension_neighbor_1);
					total_lg_matched_nodes.add(best_extension_neighbor_2);
				} else
					break;
			}

			tx.success();
		}
	}

	private void storeMatchingResults(
			Map<Long, Long> current_matched_nodes_pair, int subgraphIndex) {
		System.out.println("Saving assignments in matched subgraph("
				+ subgraphIndex + ") ...");
		Iterator<Long> itr = current_matched_nodes_pair.keySet().iterator();
		try (Transaction tx = graphDb.beginTx()) {
			while (itr.hasNext()) {
				long node_1 = itr.next();
				long node_2 = current_matched_nodes_pair.get(node_1);

				graphDb.getNodeById(node_1).setProperty("sub-graph-match",
						subgraphIndex);
				graphDb.getNodeById(node_1).setProperty("matching-node-id",
						node_2);

				graphDb.getNodeById(node_2).setProperty("sub-graph-match",
						subgraphIndex);
				graphDb.getNodeById(node_2).setProperty("matching-node-id",
						node_1);
			}
			tx.success();
		}
	}

	private static int[][] multiply(int a[][], int b[][]) {
		// Remember RC, Row-Column.
		int aRows = a.length, aColumns = a[0].length, bRows = b.length, bColumns = b[0].length;
		if (aColumns != bRows) {
			throw new IllegalArgumentException("A:Rows: " + aColumns
					+ " did not match B:Columns " + bRows + ".");
		}

		int[][] resultant = new int[aRows][bColumns];
		for (int i = 0; i < aRows; i++) { // aRow
			for (int j = 0; j < bColumns; j++) { // bColumn
				for (int k = 0; k < aColumns; k++) { // aColumn
					resultant[i][j] += a[i][k] * b[k][j];
				}
			}
		}
		return resultant;
	}

	public static void main(String args[]) {
		long[] nodes_1 = { 71415, 74207, 74271, 74161, 74202, 71443, 74141,
				74121, 74113, 74055, 73939, 73828, 73717, 73673, 73578, 73546,
				73451, 73407, 73345, 73313, 73254, 73216, 73145, 73083, 72991,
				72932, 72846, 72775, 72677, 72609, 72574, 72524, 72477, 72424,
				72374, 72315, 72280, 72245, 72198, 72136, 72050, 71976, 71911,
				71907, 71903, 71899, 71895, 71891, 71887, 71883, 71879, 71875,
				71871, 71867, 71863, 71859, 71855, 71851, 71847, 71843, 71839,
				71835, 71831, 71827, 71823, 71819, 71815, 71811, 71807, 71803,
				71799, 71795, 71791, 71787, 71783, 71779, 71775, 71771, 71767,
				71763, 71759, 71755, 71751, 71747, 71743, 71739, 71735, 71731,
				71727, 71723, 71719, 71715, 71711, 71707, 71703, 71699, 71695,
				71691, 71687, 71683, 71679, 71675, 71671, 71667, 71663, 71659,
				71655, 71651, 71647, 71643, 71639, 71635, 71631, 71627, 71623,
				71619, 71615, 71611, 71607, 71603, 71599, 71595, 71591, 71587,
				71583, 71579, 71575, 71571, 71567, 71563, 71559, 71555, 71551,
				71547, 71543, 71539, 71535, 71531, 71527, 71523, 71519, 71515,
				71511 };
		long[] nodes_2 = { 67229, 71347, 71411, 71301, 71342, 71192, 71299,
				71297, 71295, 71293, 71291, 71289, 71287, 71282, 71277, 71272,
				71267, 71101, 71190, 71188, 71186, 71181, 71176, 67257, 71081,
				71061, 71041, 71021, 71013, 70955, 70897, 70786, 70675, 70554,
				70443, 70312, 70262, 70188, 70117, 70028, 69936, 69859, 69794,
				69720, 69643, 69557, 69489, 69394, 69311, 69249, 69145, 69077,
				68985, 68860, 68753, 68634, 68524, 68471, 68379, 68290, 68207,
				68109, 68023, 67901, 67830, 67753, 67749, 67745, 67741, 67737,
				67733, 67729, 67725, 67721, 67717, 67713, 67709, 67705, 67701,
				67697, 67693, 67689, 67685, 67681, 67677, 67673, 67669, 67665,
				67661, 67657, 67653, 67649, 67645, 67641, 67637, 67633, 67629,
				67625, 67621, 67617, 67613, 67609, 67605, 67601, 67597, 67593,
				67589, 67585, 67581, 67577, 67573, 67569, 67565, 67561, 67557,
				67553, 67549, 67545, 67541, 67537, 67533, 67529, 67525, 67521,
				67517, 67513, 67509, 67505, 67501, 67497, 67493, 67489, 67485,
				67481, 67477, 67473, 67469, 67465, 67461, 67457, 67453, 67449,
				67445, 67441, 67437, 67433, 67429, 67425, 67421, 67417, 67413,
				67409, 67405, 67401, 67397, 67393, 67389, 67385, 67381, 67377,
				67373, 67369, 67365, 67361, 67357, 67353, 67349, 67345, 67341,
				67337, 67333, 67329, 67325 };
		(new SubGraphMatching(DatabaseService.getDatabaseService()))
				.performMatching(nodes_1, nodes_2, false);
	}
}
