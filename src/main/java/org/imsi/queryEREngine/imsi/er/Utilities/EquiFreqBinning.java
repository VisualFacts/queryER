package org.imsi.queryEREngine.imsi.er.Utilities;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class EquiFreqBinning extends AbstractHistogramBinning {

	public EquiFreqBinning(Map<String, Integer> tfIdf) {
		super(tfIdf);
	}

	// Divides the data into buckets of equal frequencies, the buckets have equal sizes
	// Remove from the invertedIndex the first element of each bucket characterizing it as the lead of the bucket.
	public List<List<String>> implement() { 
	    int a = tfIdf.size();
	    int m = (a / n); 
	    Object[] keys = tfIdf.keySet().toArray();
	    List<List<String>> bins = new ArrayList<>();
	    for (int i = 0; i < n; i++) { 
	        ArrayList<String> arr =  new ArrayList<>();
	        for(int j = (i*n); j < (i + 1) * n; j++){ 
	            if (j >= a) 
	                break;
	            arr.add(keys[j].toString()); 
	        }
	        bins.add(arr);
	    }
	    return bins;
	}
	
}
