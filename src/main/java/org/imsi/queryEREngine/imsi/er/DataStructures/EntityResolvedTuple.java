package org.imsi.queryEREngine.imsi.er.DataStructures;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.imsi.queryEREngine.imsi.er.Utilities.DumpDirectories;
import org.imsi.queryEREngine.imsi.er.Utilities.EntityGrouping;
import org.imsi.queryEREngine.imsi.er.Utilities.MapUtilities;
import org.imsi.queryEREngine.imsi.er.Utilities.SerializationUtilities;
import org.imsi.queryEREngine.imsi.er.Utilities.UnionFind;

public class EntityResolvedTuple<T> extends AbstractEnumerable<T> {

	public HashMap<Integer, Object[]> data;

	public UnionFind uFind;
	public HashMap<Integer, Set<Integer>> revUF; // these is the query links
	public HashMap<Integer, Set<Integer>> links; // these are the total links
	public List<T> finalData;
	private boolean isGrouped = false;
	private int matches;
	private Integer comparisons;
	private double compTime;
	private double revUFCreationTime;
	private Integer keyIndex;
	private Integer noOfAttributes;
	private DumpDirectories dumpDirectories = DumpDirectories.loadDirectories();
	
	public EntityResolvedTuple(HashMap<Integer, Object[]> data, UnionFind uFind, Integer keyIndex, Integer noOfAttributes) {
		super();
		this.data = data;
		this.uFind = uFind;
		this.finalData = new ArrayList<>();
		this.revUF = new HashMap<>();
		this.keyIndex = keyIndex;
		this.noOfAttributes = noOfAttributes;
	}
	
	public EntityResolvedTuple(List<Object[]> finalData, UnionFind uFind, Integer keyIndex, Integer noOfAttributes) {
		super();
		this.finalData = (List<T>) finalData;
		this.revUF = new HashMap<>();
	}
	

	@Override
	public Enumerator<T> enumerator() {
		if(!isGrouped) this.groupEntities();
		Enumerator<T> originalEnumerator = Linq4j.enumerator(this.finalData);
		// TODO Auto-generated method stub
		return new Enumerator<T>() {

			@Override
			public T current() {
				return originalEnumerator.current();
			}

			@Override
			public boolean moveNext() {
				while (originalEnumerator.moveNext()) {
					return true;
				}
				return false;
			}

			@Override
			public void reset() {

			}

			@Override
			public void close() {

			}

		};
	}

	@SuppressWarnings("unchecked")
	public void sortEntities() {
		// TODO Auto-generated method stub
		this.finalData = (List<T>) EntityGrouping.sortSimilar(this.revUF, this.data);	

	}
	
	@SuppressWarnings("unchecked")
	public void groupEntities() {
		this.finalData = (List<T>) EntityGrouping.groupSimilar(this.revUF, 	this.data, keyIndex, noOfAttributes);	
		isGrouped = true;

	}
	
	@SuppressWarnings("unchecked")
	public void getAll() {
		double revUFCreationStartTime = System.currentTimeMillis();
		
		for (int child : uFind.getParent().keySet()) {
			int parent = uFind.getParent().get(child);
			this.revUF.computeIfAbsent(parent, x -> new HashSet<>()).add(child);
			this.revUF.computeIfAbsent(child, x -> new HashSet<>()).add(parent);
			// For both of these go to their similarities and recompute them
			for(int simPar : this.revUF.get(parent)) {
				if(simPar != parent)
					this.revUF.computeIfAbsent(simPar, x -> new HashSet<>()).addAll(this.revUF.get(parent));
			}
			for(int simPar : this.revUF.get(child)) {
				if(simPar != child)
					this.revUF.computeIfAbsent(simPar, x -> new HashSet<>()).addAll(this.revUF.get(child));
			}
		}

		double revUFCreationEndTime = System.currentTimeMillis();
		this.setRevUFCreationTime((revUFCreationEndTime - revUFCreationStartTime)/1000);
	}
	
	public void mergeLinks(HashMap<Integer, Set<Integer>> links, String tableName, boolean firstDedup,
			Set<Integer> totalIds, boolean runLinks) {
		this.links = links;
		if(!firstDedup) this.combineLinks(links);
		if(runLinks) storeLinks(tableName);
		filterData(totalIds);	
	}
	
	public void storeLinks(String table) {
		String linksDir = dumpDirectories.getLinksDirPath();
		if(this.links == null)
			SerializationUtilities.storeSerializedObject(this.revUF, linksDir + table);
		else {
			SerializationUtilities.storeSerializedObject(this.links, linksDir + table);
			this.links.clear();
		}
	}
	
	public void storeLI() {
		SerializationUtilities.storeSerializedObject(this.revUF, dumpDirectories.getLiFilePath());
	}
	
	public void filterData(Set<Integer> totalIds) {
		HashMap<Integer, Object[]> filteredData = new HashMap<>();
		// First filter the merged revUF by keeping only the query ids + dup ids
		this.revUF.keySet().retainAll(totalIds);
		this.revUF.values().forEach(v -> v.retainAll(totalIds));
		for (int id : this.revUF.keySet()) {
			Object[] datum = this.data.get(id);
			filteredData.put(id, datum);
			this.finalData.add((T) datum);
		}
		this.data = filteredData;
	}
	
	public void combineLinks(Map<Integer, Set<Integer>> links) {
		if(revUF != null) {

			for (Entry<Integer, Set<Integer>> e : revUF.entrySet())
			    links.merge(e.getKey(), e.getValue(), (v1, v2) -> {
			    	v1.addAll(v2);
			    	return v1;
			    });
		}
		this.revUF = (HashMap<Integer, Set<Integer>>) MapUtilities.deepCopy(this.links);
	}

	public int getMatches() {
		return matches;
	}

	public void setMatches(int i) {
		this.matches = i;
	}

	public Integer getComparisons() {
		return comparisons;
	}

	public void setComparisons(Integer comparisons) {
		this.comparisons = comparisons;
	}

	public HashMap<Integer, Object[]> getData() {
		return data;
	}

	public void setData(HashMap<Integer, Object[]> data) {
		this.data = data;
	}

	public HashMap<Integer, Set<Integer>> getRevUF() {
		return revUF;
	}

	public void setRevUF(HashMap<Integer, Set<Integer>> revUF) {
		this.revUF = revUF;
	}

	public List<T> getFinalData() {
		return finalData;
	}

	public void setFinalData(List<T> finalData) {
		this.finalData = finalData;
	}

	public double getCompTime() {
		return compTime;
	}

	public void setCompTime(double compTime) {
		this.compTime = compTime;
	}

	public double getRevUFCreationTime() {
		return revUFCreationTime;
	}

	public void setRevUFCreationTime(double revUFCreationTime) {
		this.revUFCreationTime = revUFCreationTime;
	}

	public HashMap<Integer, Set<Integer>> getLinks() {
		return links;
	}



	

}
