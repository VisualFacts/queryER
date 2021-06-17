package org.imsi.queryEREngine.imsi.er.BigVizUtilities;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class BigVizStatistic implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8129873355065905104L;
	
	public double percentOfDups;
	public HashMap<String, Double> similarityMeasures;
	public LinkedHashMap<String, HashMap<String, Integer>> columnValues;
	
	public BigVizStatistic(double percentOfDups, HashMap<String, Double> similarityMeasures, LinkedHashMap<String, HashMap<String, Integer>> columnValues) {
		super();
		this.percentOfDups = percentOfDups;
		this.similarityMeasures = similarityMeasures;
		this.columnValues = columnValues;
	}
}
