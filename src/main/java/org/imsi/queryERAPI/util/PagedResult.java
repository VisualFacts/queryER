package org.imsi.queryERAPI.util;

import java.util.List;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class PagedResult {

	int pages;
	List<ObjectNode> data;
	long size;
	
	public PagedResult(int pages, List<ObjectNode> data, long size) {
		super();
		this.pages = pages;
		this.data = data;
		this.size = size;
	}

	public int getPages() {
		return pages;
	}

	public List<ObjectNode> getData() {
		return data;
	}

	public long getSize() {
		return size;
	}
	
	
}
