package org.imsi.queryEREngine.imsi.er.Utilities;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class BigVizOutput implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 464784220057275840L;
	public Integer offset;
	public HashMap<String, String> columns;		
	
	
	public BigVizOutput(Integer offset, HashMap<String, String> columns) {
		this.offset = offset;
		this.columns = columns;
	}
	
}
