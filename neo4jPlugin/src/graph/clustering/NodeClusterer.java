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


package graph.clustering;

import graph.utils.DatabaseService;
import graph.utils.GraphQuery;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.converters.ArffSaver;
import weka.clusterers.SimpleKMeans;
import weka.core.EuclideanDistance;
import weka.filters.Filter;
import weka.filters.unsupervised.attribute.StringToWordVector;
import weka.core.stemmers.SnowballStemmer;
import weka.core.tokenizers.WordTokenizer;
import weka.filters.unsupervised.attribute.RemoveUseless;

public class NodeClusterer {
	private GraphDatabaseService graphDb;

	public NodeClusterer(GraphDatabaseService graphDb) {
		this.graphDb = graphDb;
	}

	private Instances convertNodesInfoToInstances(long[] ids) {
		GraphQuery graph = new GraphQuery(graphDb);
		Map<String, String[]> nodesInfo = graph.getNodesInfo(ids);

		String[] attributeNames = new String[nodesInfo.keySet().size()];

		// Declare the feature vector
		FastVector fvWekaAttributes = new FastVector(attributeNames.length);

		int attributeIndex = 0;
		for (String attributeName : nodesInfo.keySet()) {
			attributeNames[attributeIndex++] = attributeName;
			System.out.println("Attribute:\t" + attributeName);

			Set<String> valueSet = new HashSet<String>();

			boolean isStringAttribute = false;
			String[] attributes = nodesInfo.get(attributeName);
			for (int i = 0; i < ids.length; i++) {
				valueSet.add(attributes[i]);

				if (attributes[i].split("\\s").length > 1) {
					isStringAttribute = true;
				}
			}

			Attribute wekaAttribute = null;
			if (isStringAttribute) {
				wekaAttribute = new Attribute(attributeName, (FastVector) null);
			} else {
				// Declare a nominal attribute along with its values
				FastVector fvNominalVal = new FastVector(valueSet.size());
				for (String uniqueValue : valueSet) {
					fvNominalVal.addElement(uniqueValue.toLowerCase());
				}
				wekaAttribute = new Attribute(attributeName, fvNominalVal);
			}

			// add this new attribute type to the feature vector
			fvWekaAttributes.addElement(wekaAttribute);

		}

		// Create an empty training set
		Instances clusterTrainingSet = new Instances("Rel", fvWekaAttributes,
				ids.length);
		for (int i = 0; i < ids.length; i++) {
			// Create the instance
			Instance instance = new Instance(attributeNames.length);

			for (int j = 0; j < attributeNames.length; j++) {
				String attributeValue = nodesInfo.get(attributeNames[j])[i];
				if (attributeValue == null) {
					attributeValue = "none";
				} else {
					attributeValue = attributeValue.toLowerCase();
				}

				instance.setValue((Attribute) fvWekaAttributes.elementAt(j),
						attributeValue);
			}

			// add the instance to the data set
			clusterTrainingSet.add(instance);
		}

		return clusterTrainingSet;
	}

	private Instances preprocessNodesInfoInstances(Instances clusterTrainingSet) {
		String[] filterOptions = new String[10];
		filterOptions[0] = "-R"; // attribute indices
		filterOptions[1] = "first-last";
		filterOptions[2] = "-W"; // The number of words (per class if there is a
									// class attribute assigned) to attempt to
									// keep.
		filterOptions[3] = "1000";
		filterOptions[4] = "-prune-rate"; // periodical pruning
		filterOptions[5] = "-1.0";
		filterOptions[6] = "-N"; // 0=not normalize
		filterOptions[7] = "0";
		filterOptions[8] = "-M"; // The minimum term frequency
		filterOptions[9] = "1";

		SnowballStemmer stemmer = new SnowballStemmer();
		stemmer.setStemmer("english");
		WordTokenizer tokenizer = new WordTokenizer();

		StringToWordVector s2wFilterer = new StringToWordVector();
		try {
			s2wFilterer.setOptions(filterOptions);
			s2wFilterer.setStemmer(stemmer);
			s2wFilterer.setTokenizer(tokenizer);
			s2wFilterer.setInputFormat(clusterTrainingSet);
			clusterTrainingSet = Filter.useFilter(clusterTrainingSet,
					s2wFilterer);
		} catch (Exception e1) {
			System.out.println("Error in converting string into word vectors:");
			e1.printStackTrace();
		}

		RemoveUseless ruFilter = new RemoveUseless();
		try {
			ruFilter.setInputFormat(clusterTrainingSet);
			clusterTrainingSet = Filter.useFilter(clusterTrainingSet, ruFilter);
		} catch (Exception e1) {
			System.out.println("Error in removing useless terms:");
			e1.printStackTrace();
		}

		return clusterTrainingSet;
	}

	private int[] performClustering(Instances clusterTrainingSet, int numOfClusters) {
		String[] options = new String[7];
		options[0] = "-N"; // num of clusters
		options[1] = String.valueOf(numOfClusters);
		options[2] = "-I"; // max num of iterations
		options[3] = "500";
		options[4] = "-S"; // the random seed number
		options[5] = "10";
		options[6] = "-O"; // preserve instance order

		String[] distanceOptions = new String[2];
		distanceOptions[0] = "-R"; // attribute indices
		distanceOptions[1] = "first-last";

		EuclideanDistance distanceFunc = new EuclideanDistance();
		SimpleKMeans clusterer = new SimpleKMeans();
		int[] assignments = null;
		try {
			distanceFunc.setOptions(distanceOptions);

			clusterer.setOptions(options);
			clusterer.setDistanceFunction(distanceFunc);
			clusterer.buildClusterer(clusterTrainingSet);

			assignments = clusterer.getAssignments();
		} catch (Exception e1) {
			System.out.println("Error in clustering:");
			e1.printStackTrace();
		}

		return assignments;
	}

	/*
	 * @param ids --array of node ids
	 * 
	 * @param numOfClusters --number of clusters to generate
	 * 
	 * @param storeResult --if true, store the result back as node attributes to
	 * the neo4j database
	 */
	public int[] run(long[] ids, int numOfClusters, boolean storeResult) {
		// Create the training set
		Instances clusterTrainingSet = convertNodesInfoToInstances(ids);
		clusterTrainingSet = preprocessNodesInfoInstances(clusterTrainingSet);
		int[] assignments = performClustering(clusterTrainingSet, numOfClusters);

		if (assignments != null) {
			for (int i = 0; i < ids.length; i++) {
				System.out.println(ids[i] + "--> Cluster_" + assignments[i]);
			}
		}

		if (storeResult) {
			storeMatchingResults(ids, assignments);
		}

		return assignments;
	}

	private void storeMatchingResults(long[] ids, int[] assignments) {
		System.out.println("Saving result to db ...");
		try (Transaction tx = graphDb.beginTx()) {
			for (int i = 0; i < ids.length; i++) {
				graphDb.getNodeById(ids[i]).setProperty("cluster_assignment",
						assignments[i]);
			}
			tx.success();
		}
	}
	
	public static void main(String[] args) {
		GraphDatabaseService db = DatabaseService.getDatabaseService();
		NodeClusterer clusterer = new NodeClusterer(db);

		List<Long> nodeIds = (new GraphQuery(db)).getSiteNode();
		long[] ids = new long[nodeIds.size()];
		for (int i = 0; i < ids.length; i++) {
			ids[i] = nodeIds.get(i);
		}

		// Create the training set
		Instances clusterTrainingSet = clusterer
				.convertNodesInfoToInstances(ids);
		clusterTrainingSet = clusterer
				.preprocessNodesInfoInstances(clusterTrainingSet);
		int[] assignments = clusterer.performClustering(clusterTrainingSet, 10);

		if (assignments != null) {
			for (int i = 0; i < ids.length; i++) {
				System.out.println(ids[i] + "--> Cluster_" + assignments[i]);
			}
		}

		clusterer.storeMatchingResults(ids, assignments);

		ArffSaver saver = new ArffSaver();
		saver.setInstances(clusterTrainingSet);
		try {
			saver.setFile(new File("./data/test.arff"));
			saver.writeBatch();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}
