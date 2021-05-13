package org.imsi.queryEREngine.imsi.er.Utilities;

import java.io.Serializable;
import java.util.List;

public class BigVizCluster implements Serializable  {



	private static final long serialVersionUID = -7740237215045582966L;
	public List<BigVizData> bigVizData;
	public Object[] groupedObj;
	
	public BigVizCluster(List<BigVizData> bigVizData, Object[] groupedObj) {
		this.bigVizData = bigVizData;
		this.groupedObj = groupedObj;
	}

}
