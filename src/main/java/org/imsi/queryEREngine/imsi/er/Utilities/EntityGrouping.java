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
			HashMap<Integer, Object[]> newData, Integer keyIndex, Integer noOfFields) {
		// TODO Auto-generated method stub
		List<Object[]> finalData = new ArrayList<>();
		Set<Integer> checked = new HashSet<>();
		double startTime = System.currentTimeMillis();
		for (int id : revUF.keySet()) {
			Object[] groupedObj = new Object[noOfFields]; //length
			Set<Integer> similar = revUF.get(id);
			if(checked.contains(id)) continue;
			checked.addAll(similar);
			for (int idInner : similar) {
				Object[] datum = newData.get(idInner);
				int i = 0;
				if(datum != null) {
					while(i < noOfFields) {
						
						if(groupedObj[i] == null && !(datum[i].equals("") || datum[i].equals("[\\W_]"))) {
							groupedObj[i] = datum[i];
						}
						else if (groupedObj[i] != null) {
							if(!groupedObj[i].toString().contains(datum[i].toString()) && !(datum[i].equals("") || datum[i].equals("[\\W_]")))
								groupedObj[i] = groupedObj[i].toString() + " | "+ datum[i].toString();
						}
						else {
							groupedObj[i] = null;
						}
						i++;
					}
				}
			}
			finalData.add(groupedObj);
		}
		revUF.clear();
		newData.clear();
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
