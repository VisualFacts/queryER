package org.imsi.queryEREngine.imsi.er.Utilities;

import java.util.HashMap;

public class OffsetIdsMap {
	public HashMap<Integer, Integer> offsetToId;
	public HashMap<Integer, Integer> idToOffset;
	
	
	public OffsetIdsMap(HashMap<Integer, Integer> offsetToId, HashMap<Integer, Integer> idToOffset) {
		this.offsetToId = offsetToId;
		this.idToOffset = idToOffset;
	}
}
