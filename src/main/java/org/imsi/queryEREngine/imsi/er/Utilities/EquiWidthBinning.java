package org.imsi.queryEREngine.imsi.er.Utilities;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class EquiWidthBinning extends AbstractHistogramBinning {

	// Divides the data into buckets of equal width, the bucket sizes are unequal
	// In this case if a bucket is of size 1 we remove the element from the invertedIndex
	// n = number of bins
	
	public EquiWidthBinning(Map<String, Integer> tfIdf) {
		super(tfIdf);
	}

	public Set<String> implement() { 
		Object[] keys = tfIdf.keySet().toArray();
		float max = tfIdf.get(keys[0]);
		float min = tfIdf.get(keys[keys.length - 1]);

		float w = (max - min) / n; 
		Set<String> leads = new HashSet<>();
		int binSum = 0;
		List<String> binKeys = new ArrayList<>();
		for(Object key : keys) {
			String keyString = key.toString();
			binKeys.add(keyString);
			binSum += tfIdf.get(keyString);
			if(binSum >= w) {
				//for(String key2 : binKeys) { System.out.print(tfIdf.get(key2) + " "); } System.out.println();
				if(binKeys.size() > 1)
					leads.addAll(binKeys);
				binSum = 0;
				binKeys = new ArrayList<>();
				continue;
			}

		}
		return leads;
	}
}
