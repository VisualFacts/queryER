package org.imsi.queryEREngine.imsi.er.Utilities;

import java.io.ObjectInputStream.GetField;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.imsi.queryEREngine.imsi.er.BigVizUtilities.BigVizCluster;
import org.imsi.queryEREngine.imsi.er.BigVizUtilities.BigVizData;
import org.imsi.queryEREngine.imsi.er.BigVizUtilities.BigVizOutput;
import org.imsi.queryEREngine.imsi.er.BigVizUtilities.BigVizStatistic;



/**
 * 
 * @author bstam
 * Utility functions to merge an enumerable and a reverse UnionFind into merged entities.
 */
public class EntityGrouping {
	

	public static List<Object[]> groupSimilar(HashMap<Integer, Set<Integer>> revUF, 
			HashMap<Integer, Object[]> newData, Integer keyIndex, Integer noOfFields, List<Integer> projects, List<String> fieldNames, String storeLI) {

		List<Object[]> finalData = new ArrayList<>();
		Set<Integer> checked = new HashSet<>();
		double startTime = System.currentTimeMillis();
		List<BigVizCluster> bigVizDataset = new ArrayList<>();
		if(fieldNames != null) noOfFields = fieldNames.size();
		List<HashMap<String, Double>>  columnSimilarities = new ArrayList<>();
		for (int id : revUF.keySet()) {
			List<BigVizData> entityGroup = new ArrayList<>();
			Object[] groupedObj = new Object[noOfFields]; //length
			Set<Integer> similar = revUF.get(id);
			if(checked.contains(id)) continue;
			checked.addAll(similar);
			HashMap<String, Double>  clusterColumnSimilarity = new HashMap<>();
			for (int idInner : similar) {
				HashMap<String, String>  columns = new HashMap<>();
				Object[] datum = newData.get(idInner);
				if(datum != null) {
					for(int j = 0; j < noOfFields; j++) {
						int i = j;
						if(fieldNames != null) {
							i = projects.get(j);
							columns.put(fieldNames.get(j), datum[i].toString()); // for json
						}
						if(groupedObj[j] == null && !(datum[i].equals("") || datum[i].equals("[\\W_]"))) {
							groupedObj[j] = datum[i];
							clusterColumnSimilarity.put(fieldNames.get(j), 0.0);
						}
						else if (groupedObj[j] != null) {
							if(!groupedObj[j].toString().contains(datum[i].toString()) && !(datum[i].equals("") || datum[i].equals("[\\W_]"))) {
								groupedObj[j] = groupedObj[j].toString() + " | "+ datum[i].toString();
								clusterColumnSimilarity.put(fieldNames.get(j), clusterColumnSimilarity.get(fieldNames.get(j)) + 1);
							}
						}
						else {
							groupedObj[j] = null;
						}
					}
					
				}
				entityGroup.add(new BigVizData(idInner, columns));
			}
			clusterColumnSimilarity.replaceAll((k,v) -> v != null ? v / entityGroup.size() : null);
			columnSimilarities.add(clusterColumnSimilarity);
			similar.remove(id);
			BigVizCluster cluster = new BigVizCluster(entityGroup, groupedObj, clusterColumnSimilarity);
			if(similar.size() > 0) bigVizDataset.add(cluster);
			finalData.add(groupedObj);
		}
		revUF.clear();
		newData.clear();
		BigVizStatistic bigVizStatistic = generateBigVizStatistic(bigVizDataset, columnSimilarities, finalData.size());
		BigVizOutput bigVizOutput = new BigVizOutput(bigVizDataset, bigVizStatistic);
		System.out.println(bigVizStatistic);
		SerializationUtilities.storeSerializedObject(bigVizOutput, storeLI);
		double endTime = System.currentTimeMillis();
        String totalTime = Double.toString((endTime -startTime) / 1000);
        System.out.println("Grouping time: " + totalTime);
		return finalData;
	}


	private static BigVizStatistic generateBigVizStatistic(List<BigVizCluster> bigVizDataset,
			List<HashMap<String, Double>> columnSimilarities, int size) {
		double percentOfDups = (double) bigVizDataset.size() / (double) size;
		Map<String, Double> avgColumSimilarities = new HashMap<>();
		avgColumSimilarities = columnSimilarities.stream()
			    .flatMap(map -> map.entrySet().stream())
			    .collect(Collectors.groupingBy(Map.Entry::getKey, 
			             Collectors.averagingDouble(value -> (value.getValue()))));
		
		BigVizStatistic bigVizStatistic = new BigVizStatistic(percentOfDups, (HashMap<String, Double>) avgColumSimilarities);
		return bigVizStatistic;
	}


	public static List<Object[]> sortSimilar(HashMap<Integer, Set<Integer>> revUF, HashMap<Integer, Object[]> newData) {
		// TODO Auto-generated method stub
		List<Object[]> finalData = new ArrayList<>();
		
		for (int id : revUF.keySet()) {
			for (int idInner : revUF.get(id)) {
				finalData.add(newData.get(idInner));
			}
		}
		revUF.clear();
		newData.clear();
		return finalData;
	}

}
