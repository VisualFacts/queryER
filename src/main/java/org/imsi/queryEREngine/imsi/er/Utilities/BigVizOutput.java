package org.imsi.queryEREngine.imsi.er.Utilities;

import java.io.Serializable;
import java.util.List;

public class BigVizOutput implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6208168040968847318L;

	public double percentOfDups = 0.0;
	public List<BigVizCluster> bigVizClusters;
	public BigVizOutput(double percentOfDups, List<BigVizCluster> bigVizClusters) {
		super();
		this.percentOfDups = percentOfDups;
		this.bigVizClusters = bigVizClusters;
	}
	
	
	
}
