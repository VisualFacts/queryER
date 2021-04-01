package org.imsi.queryEREngine.imsi.er.DataStructures;

import java.util.List;
import java.util.Set;

public class JoinFilterTuple {

	protected Set<Integer> ids;
	protected List<Object[]> data;
	
	public JoinFilterTuple(Set<Integer> ids, List<Object[]> data) {
		super();
		this.ids = ids;
		this.data = data;
	}
	public Set<Integer> getIds() {
		return ids;
	}
	public void setIds(Set<Integer> ids) {
		this.ids = ids;
	}
	public List<Object[]> getData() {
		return data;
	}
	public void setData(List<Object[]> data) {
		this.data = data;
	}
	
}
