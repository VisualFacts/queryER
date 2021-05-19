package org.imsi.queryEREngine.imsi.er.BigVizUtilities;

import java.io.Serializable;
import java.util.List;

public class BigVizOutput implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6208168040968847318L;

	public double percentOfDups = 0.0;
	public BigVizStatistic bigVizStatistic;
	public List<BigVizCluster> bigVizDataset;
	
	public BigVizOutput(List<BigVizCluster> bigVizDataset, BigVizStatistic bigVizStatistic) {
		super();
		this.bigVizDataset = bigVizDataset;
		this.bigVizStatistic = bigVizStatistic;
	}
	
	
	
}
