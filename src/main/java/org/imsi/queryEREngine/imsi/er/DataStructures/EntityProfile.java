package org.imsi.queryEREngine.imsi.er.DataStructures;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class EntityProfile implements Serializable {

	private static final long serialVersionUID = 122354534453243447L;

	private final Set<Attribute> attributes;
	private final String entityUrl;

	public EntityProfile(String url) {
		entityUrl = url;
		attributes = new HashSet();
	}

	public void addAttribute(Integer propertyPos, String propertyValue) {
		attributes.add(new Attribute(propertyPos, propertyValue));
	}

	public String getEntityUrl() {
		return entityUrl;
	}

	public int getProfileSize() {
		return attributes.size();
	}

	public Set<Attribute> getAttributes() {
		return attributes;
	}
}