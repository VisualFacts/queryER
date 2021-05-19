package org.imsi.queryEREngine.imsi.er.BigVizUtilities;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;

public class BigVizCluster implements Serializable  {



	private static final long serialVersionUID = -7740237215045582966L;
	public List<BigVizData> bigVizData;
	public Object[] groupedObj;
	public HashMap<String, Double> clusterColumnSimilarity;
	
	public BigVizCluster(List<BigVizData> bigVizData, Object[] groupedObj, HashMap<String, Double> clusterColumnSimilarity) {
		this.bigVizData = bigVizData;
		this.groupedObj = groupedObj;
		this.clusterColumnSimilarity = clusterColumnSimilarity;
	}

}
