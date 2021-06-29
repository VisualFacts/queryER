package org.imsi.queryEREngine.imsi.er.Utilities;

import java.io.ObjectInputStream.GetField;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.imsi.queryEREngine.imsi.er.BigVizUtilities.BigVizCluster;
import org.imsi.queryEREngine.imsi.er.BigVizUtilities.BigVizData;
import org.imsi.queryEREngine.imsi.er.BigVizUtilities.BigVizOutput;
import org.imsi.queryEREngine.imsi.er.BigVizUtilities.BigVizStatistic;
import org.imsi.queryEREngine.imsi.er.Utilities.SerializationUtilities;


/**
 * 
 * @author bstam
 * Utility functions to merge an enumerable and a reverse UnionFind into merged entities.
 */
public class EntityGrouping {
	

	public static List<Object[]> groupSimilarAll(HashMap<Integer, Set<Integer>> revUF, 
			HashMap<Integer, Object[]> newData, Integer keyIndex, Integer noOfFields, List<Integer> projects, List<String> fieldNames, String storeLI) {

		List<Object[]> finalData = new ArrayList<>();
		Set<Integer> checked = new HashSet<>();
		double startTime = System.currentTimeMillis();
		List<BigVizCluster> bigVizDataset = new ArrayList<>();
		if(fieldNames != null) noOfFields = fieldNames.size();
		//List<HashMap<String, Double>>  columnSimilarities = new ArrayList<>(); // List of the column similarities of each cluster
		for (int id : revUF.keySet()) {
			List<BigVizData> entityGroup = new ArrayList<>();
			Object[] groupedObj = new Object[noOfFields]; //length
			Set<Integer> similar = revUF.get(id);
			if(checked.contains(id)) continue;
			checked.addAll(similar);
			//HashMap<String, Double>  clusterColumnSimilarity = new HashMap<>();
			for (int idInner : similar) {
				//HashMap<String, String>  columns = new HashMap<>();
				Object[] datum = newData.get(idInner);
				if(datum != null) {
					for(int j = 0; j < noOfFields; j++) {
						int i = j;
						if(fieldNames != null) {
							i = projects.get(j);
							//columns.put(fieldNames.get(j), datum[i].toString()); // for json
						}
						if(groupedObj[j] == null && !(datum[i].equals("") || datum[i].equals("[\\W_]"))) {
							groupedObj[j] = datum[i];
						}
						else if (groupedObj[j] != null) {
							if(!groupedObj[j].toString().contains(datum[i].toString()) && !(datum[i].equals("") || datum[i].equals("[\\W_]"))) {
								groupedObj[j] = groupedObj[j].toString() + " | "+ datum[i].toString();
								//clusterColumnSimilarity.merge(fieldNames.get(j), 2.0, (a, b) ->  a + 1);
							}
						}
						else {
							groupedObj[j] = null;
						}
					}
					
				}
				//entityGroup.add(new BigVizData(idInner, columns));
			}
			
//			similar.remove(id);
//			if(similar.size() > 0) {
//				clusterColumnSimilarity.replaceAll((k,v) -> v != null ? v / entityGroup.size() : 0.0);
//				for(String col : fieldNames) clusterColumnSimilarity.putIfAbsent(col, 0.0);
//				columnSimilarities.add(clusterColumnSimilarity);
//				BigVizCluster cluster = new BigVizCluster(entityGroup, groupedObj, clusterColumnSimilarity);
//				bigVizDataset.add(cluster);
//			}
			finalData.add(groupedObj);
		}
		revUF.clear();
		newData.clear();
//		BigVizStatistic bigVizStatistic = generateBigVizStatistic(bigVizDataset, columnSimilarities, finalData.size());
//		BigVizOutput bigVizOutput = new BigVizOutput(bigVizDataset, bigVizStatistic);
//		SerializationUtilities.storeSerializedObject(bigVizOutput, storeLI);
		double endTime = System.currentTimeMillis();
        String totalTime = Double.toString((endTime -startTime) / 1000);
        System.out.println("Grouping time: " + totalTime);
		return finalData;
	}

	public static List<Object[]> groupSimilar(HashMap<Integer, Set<Integer>> revUF, 
			HashMap<Integer, Object[]> newData, HashMap<Integer, HashMap<Integer, Double>> similarities, 
			Integer keyIndex, Integer noOfFields, List<Integer> projects, List<String> fieldNames, String storeLI) {
		
		if(fieldNames == null) return groupSimilarAll(revUF, newData, keyIndex, noOfFields, projects, fieldNames, storeLI);

		
		List<Object[]> finalData = new ArrayList<>();
		Set<Integer> checked = new HashSet<>();
		double startTime = System.currentTimeMillis();
		List<BigVizCluster> bigVizDataset = new ArrayList<>();
		if(fieldNames != null) noOfFields = fieldNames.size();
		List<HashMap<String, Double>>  columnSimilarities = new ArrayList<>(); // List of the column similarities of each cluster
		LinkedHashMap<String, HashMap<String,Integer>> clustersColumnValues = new LinkedHashMap<>();
		for (int id : revUF.keySet()) {
			List<BigVizData> entityGroup = new ArrayList<>();
			Set<Integer> similar = revUF.get(id);
			/* Because we resolve all duplicates when found the first id of the cluster we use this set */
			if(checked.contains(id)) continue; 
			checked.addAll(similar);
			HashMap<String, Double>  clusterColumnSimilarity = new HashMap<>(); // This cluster's column similarities
			LinkedHashMap<String, HashMap<String, Integer>> clusterColumns = new LinkedHashMap<>(); // Columns of this cluster
			for (int idInner : similar) {
				HashMap<String, String>  columns = new HashMap<>();
				Object[] datum = newData.get(idInner);
				if(datum != null) {
					for(int j = 0; j < noOfFields; j++) {
						int i = j;
						String col = fieldNames.get(j);
						i = projects.get(j);
						String value = datum[i].toString();
						columns.put(col, value); // for json
						HashMap<String, Integer> valueFrequencies = clusterColumns.computeIfAbsent(col, x -> new HashMap<>());
						int valueFrequency = valueFrequencies.containsKey(value) ? valueFrequencies.get(value) : 0;
						if(!value.equals("") && !datum[i].equals("[\\W_]"))
							valueFrequencies.put(value, valueFrequency + 1);
						
						/* If there are duplicates we get the frequencies of the values of this cluster */
						if(similar.size() > 1) {
							HashMap<String, Integer> valueFrequenciesDup = clustersColumnValues.computeIfAbsent(col, x -> new HashMap<>());
							int valueFrequencyDup = valueFrequenciesDup.containsKey(value) ? valueFrequenciesDup.get(value) : 0;
							if(!value.equals("") && !datum[i].equals("[\\W_]"))
								valueFrequenciesDup.put(value, valueFrequencyDup + 1);
						}
					}
					
				}
				entityGroup.add(new BigVizData(idInner, columns));
			}
			
			similar.remove(id);
			Object[] groupedObject = clusterToString(clusterColumns); // Creates the grouped object from the columns map
			/* If there are duplicates we compute the statistics of the cluster */
			if(similar.size() > 0) {
				clusterColumnSimilarity = (HashMap<String, Double>) getDistanceMeasure(clusterColumns);
				for(String col : fieldNames) clusterColumnSimilarity.putIfAbsent(col, 0.0);
				Map<Integer, HashMap<Integer, Double>> clusterSimilarities  = new HashMap<>();
				columnSimilarities.add(clusterColumnSimilarity);
				if(similarities != null)
					similar.stream()
					    .filter(similarities::containsKey)
					    .collect(Collectors.toMap(Function.identity(), similarities::get));
				BigVizCluster cluster = new BigVizCluster(entityGroup, clusterColumnSimilarity, clusterColumns, clusterSimilarities, groupedObject);
				bigVizDataset.add(cluster);				
				
			}
			finalData.add(groupedObject);
		}
		revUF.clear();
		newData.clear();
		BigVizStatistic bigVizStatistic = generateBigVizStatistic(bigVizDataset, columnSimilarities, clustersColumnValues, finalData.size());
		BigVizOutput bigVizOutput = new BigVizOutput(bigVizDataset, bigVizStatistic);
		SerializationUtilities.storeSerializedObject(bigVizOutput, storeLI);
		double endTime = System.currentTimeMillis();
        String totalTime = Double.toString((endTime -startTime) / 1000);
        System.out.println("Grouping time: " + totalTime);
		return finalData;
	}
	
	static Map<String, Double> getDistanceMeasure(HashMap<String, HashMap<String, Integer>> clusterColumns){
		
		Map<String, Double> distMeasures = clusterColumns.entrySet().stream()
			.collect(Collectors.toMap(Entry::getKey, e -> {
				Object[] keys = e.getValue().keySet().toArray();
				return elementWiseJaro(keys);
			}));
		return distMeasures;
		
	}
	
	static Double elementWiseJaro(Object[] vals) {
		double avg = 0.0;
		int size = vals.length;
		for(int i = 0; i < size; i ++) {
			for(int j = i + 1; j < size; j++) {
				avg += ProfileComparison.jaro(vals[i].toString(), vals[j].toString());
			}
		}
		return avg / size;
	}
	
	static Object[] clusterToString(HashMap<String, HashMap<String, Integer>> clusterColumns) {
		Object[] columnValues = clusterColumns.values().stream().map(v -> {
			Object[] keys = v.keySet().toArray();
		
			String colVal = "";
			int sz = keys.length;
			if(sz > 0) colVal = keys[0].toString(); 
			for(int i = 1; i < keys.length; i++) {
				colVal += " | " + keys[i].toString();
			}
			
			return colVal;
		}).toArray();
//		for(int i = 0; i < columnValues.length; i ++) System.out.print(columnValues[i] + ", ") ;
//		System.out.println();
		return columnValues;
		
	}
	
	private static BigVizStatistic generateBigVizStatistic(List<BigVizCluster> bigVizDataset,
			List<HashMap<String, Double>> columnSimilarities,
			LinkedHashMap<String, HashMap<String, Integer>> columnValues, int size) {
		double percentOfDups = (double) bigVizDataset.size() / (double) size;
		Map<String, Double> avgColumSimilarities = new HashMap<>();
		avgColumSimilarities = columnSimilarities.stream()
			    .flatMap(map -> map.entrySet().stream())
			    .collect(Collectors.groupingBy(Map.Entry::getKey, 
			             Collectors.averagingDouble(value -> (value.getValue()))));

		BigVizStatistic bigVizStatistic = new BigVizStatistic(percentOfDups, (HashMap<String, Double>) avgColumSimilarities, columnValues);
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
