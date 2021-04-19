package org.imsi.queryEREngine.imsi.er.Utilities;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Set;

public class BigVizOutput implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6124207876075174783L;
	public Integer offset;
	public HashMap<String, String> columns;
	public Set<Integer> similarOffsets;
	
	
	public BigVizOutput(Integer offset, HashMap<String, String> columns, Set<Integer> similarOffsets) {
		this.offset = offset;
		this.columns = columns;
		this.similarOffsets = similarOffsets;
	}
}
