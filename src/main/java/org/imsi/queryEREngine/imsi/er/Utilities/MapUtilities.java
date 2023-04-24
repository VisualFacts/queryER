package org.imsi.queryEREngine.imsi.er.Utilities;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class MapUtilities {
	public static <U, T> Map<T, Set<U>> deepCopy(Map<T, Set<U>> map) {
		Map<T, Set<U>> clone = new LinkedHashMap<>();
		for (Map.Entry<T, Set<U>> e : map.entrySet())
			clone.put(e.getKey(), e.getValue().stream().collect(Collectors.toSet())); 
		return clone;
	}
	
	public static Set<Integer> deepCopySet(Set<Integer> set){
//		return set.stream().map(Integer::new).collect(Collectors.toSet());
//		return set.stream().map(Integer::valueOf).collect(Collectors.toSet());
		return set.stream().collect(Collectors.toSet());
	}
}
