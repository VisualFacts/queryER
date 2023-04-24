/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.imsi.queryEREngine.imsi.calcite.adapter.enumerable.csv;

import java.util.ArrayList;

import java.util.List;


import org.imsi.queryEREngine.apache.calcite.adapter.java.JavaTypeFactory;
import org.imsi.queryEREngine.apache.calcite.rel.RelCollation;
import org.imsi.queryEREngine.apache.calcite.rel.RelDistribution;
import org.imsi.queryEREngine.apache.calcite.rel.RelDistributionTraitDef;
import org.imsi.queryEREngine.apache.calcite.rel.RelReferentialConstraint;
import org.imsi.queryEREngine.apache.calcite.rel.type.RelDataType;
import org.imsi.queryEREngine.apache.calcite.rel.type.RelDataTypeFactory;
import org.imsi.queryEREngine.apache.calcite.rel.type.RelProtoDataType;
import org.imsi.queryEREngine.apache.calcite.rex.RexNode;
import org.imsi.queryEREngine.apache.calcite.schema.Statistic;
import org.imsi.queryEREngine.apache.calcite.schema.impl.AbstractTable;
import org.imsi.queryEREngine.apache.calcite.util.ImmutableBitSet;
import org.imsi.queryEREngine.apache.calcite.util.Source;
import org.imsi.queryEREngine.imsi.er.KDebug;
import com.google.common.collect.ImmutableList;


/**
 * Base class for table that reads CSV files.
 */
public abstract class CsvTable extends AbstractTable {
	protected final Source source;
	protected final RelProtoDataType protoRowType;
	protected List<CsvFieldType> fieldTypes;
	protected RelDataType fullTypes;
	protected int tableKey;
	protected int tableSize;
	protected String tableName;
	protected Double rows;
	protected Boolean statsComputed;
	protected CsvTableStatistic csvTableStatistic;

	/** Creates a CsvTable. */
	CsvTable(Source source, String name, RelProtoDataType protoRowType) {
		this.source = source;
		this.tableName = name;
		this.protoRowType = protoRowType;

	}

	@Override
	public RelDataType getRowType(RelDataTypeFactory typeFactory) {
		if (protoRowType != null) {
			return protoRowType.apply(typeFactory);
		}
		if (fieldTypes == null) {
			fieldTypes = new ArrayList<>();
			fullTypes =	CsvEnumerator.deduceRowType((JavaTypeFactory) typeFactory, source,
					fieldTypes);
			return fullTypes;
		} else {
			fullTypes = CsvEnumerator.deduceRowType((JavaTypeFactory) typeFactory, source,
					null);
			return fullTypes;
		}
	}

	public String getName() {
		return this.tableName;
	}
	public void setKey(int keyFieldIndex) {
		this.tableKey = keyFieldIndex;
	}

	public int getKey() {
		return this.tableKey;
	}


	@Override  public Statistic getStatistic() {

		return new Statistic() {
			
			@Override
			public Double getRowCount() {
				if(rows == null)
					//rows = Double.parseDouble(csvTableStatistic.columnCardinalities.get(tableKey).toString());
					rows = 1000.0;
				//System.out.println("Stats" + " " +rows);
				return rows;
			}

			@Override
			public boolean isKey(ImmutableBitSet columns) {
				return false;
			}

			@Override
			public List<ImmutableBitSet> getKeys() {
				return ImmutableList.of();
			}

			@Override
			public List<RelReferentialConstraint> getReferentialConstraints() {
				return ImmutableList.of();
			}

			@Override
			public List<RelCollation> getCollations() {
				return ImmutableList.of();
			}

			@Override
			public RelDistribution getDistribution() {
				return RelDistributionTraitDef.INSTANCE.getDefault();
			}

			@Override
			public Double getComparisons(List<RexNode> paramList,  String tableName) {
				// TODO Auto-generated method stub
				return null;
			}	
		};
	}
	// For debugging
		public static String getCallerClassName() {
			StackTraceElement[] stElements = Thread.currentThread().getStackTrace();
			String callerClassName = null;
			for (int i=1; i<stElements.length; i++) {
				StackTraceElement ste = stElements[i];
				if (!ste.getClassName().equals(KDebug.class.getName())&& ste.getClassName().indexOf("java.lang.Thread")!=0) {
					if (callerClassName==null) {
						callerClassName = ste.getClassName();
					} else if (!callerClassName.equals(ste.getClassName())) {
						return ste.getClassName();
					}
				}
			}
			return null;
		}

	public Source getSource() {
		return source;
	}

	public List<CsvFieldType> getFieldTypes() {
		return fieldTypes;
	}

	public void setFieldTypes(List<CsvFieldType> fieldTypes) {
		this.fieldTypes = fieldTypes;
	}
	
	public Boolean getStatsComputed() {
		return statsComputed;
	}

	public void setStatsComputed(Boolean statsComputed) {
		this.statsComputed = statsComputed;
	}
	/** Various degrees of table "intelligence". */
	public enum Flavor {
		TRANSLATABLE
	}
	public CsvTableStatistic getCsvTableStatistic() {
		return csvTableStatistic;
	}

	public void setCsvTableStatistic(CsvTableStatistic csvTableStatistic) {
		this.csvTableStatistic = csvTableStatistic;
	}

	public int getTableSize() {
		return tableSize;
	}

	public void setTableSize(int tableSize) {
		this.tableSize = tableSize;
	}
}

// End CsvTable.java
