package org.imsi.queryEREngine.imsi.er.Utilities;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;



/**
 * 
 * @author bstam
 * Utility functions to merge an enumerable and a reverse UnionFind into merged entities.
 */
public class EntityGrouping {
	

	public static List<Object[]> groupSimilar(HashMap<Integer, Set<Integer>> revUF, 
			HashMap<Integer, Object[]> newData, Integer keyIndex, Integer noOfFields, List<Integer> projects, List<String> fieldNames, String storeLI) {
		// TODO Auto-generated method stub
		List<Object[]> finalData = new ArrayList<>();
		Set<Integer> checked = new HashSet<>();
		double startTime = System.currentTimeMillis();
		List<BigVizCluster> bigVizDataset = new ArrayList<>();
		if(fieldNames != null) noOfFields = fieldNames.size();
		for (int id : revUF.keySet()) {
			List<BigVizData> entityGroup = new ArrayList<>();
			Object[] groupedObj = new Object[noOfFields]; //length
			Set<Integer> similar = revUF.get(id);
			if(checked.contains(id)) continue;
			checked.addAll(similar);
			HashMap<String, String>  columns = new HashMap<>();
			for (int idInner : similar) {
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
						}
						else if (groupedObj[j] != null) {
							if(!groupedObj[j].toString().contains(datum[i].toString()) && !(datum[i].equals("") || datum[i].equals("[\\W_]")))
								groupedObj[j] = groupedObj[j].toString() + " | "+ datum[i].toString();
						}
						else {
							groupedObj[j] = null;
						}
					}
				}
				entityGroup.add(new BigVizData(idInner, columns));
			}
			similar.remove(id);
			BigVizCluster cluster = new BigVizCluster(entityGroup, groupedObj);
			if(similar.size() > 0) bigVizDataset.add(cluster);
			finalData.add(groupedObj);
		}
		revUF.clear();
		newData.clear();
		double percentOfDups = (double) bigVizDataset.size() / (double) finalData.size();
		BigVizOutput bigVizOutput = new BigVizOutput(percentOfDups, bigVizDataset);
		SerializationUtilities.storeSerializedObject(bigVizOutput, storeLI);
		double endTime = System.currentTimeMillis();
        String totalTime = Double.toString((endTime -startTime) / 1000);
        System.out.println("Grouping time: " + totalTime);
		return finalData;
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
