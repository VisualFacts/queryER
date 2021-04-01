/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.imsi.queryEREngine.apache.calcite.adapter.enumerable;

import java.util.function.Predicate;

import org.imsi.queryEREngine.apache.calcite.plan.Convention;
import org.imsi.queryEREngine.apache.calcite.plan.RelOptTable;
import org.imsi.queryEREngine.apache.calcite.rel.RelNode;
import org.imsi.queryEREngine.apache.calcite.rel.convert.ConverterRule;
import org.imsi.queryEREngine.apache.calcite.rel.core.RelFactories;
import org.imsi.queryEREngine.apache.calcite.rel.logical.LogicalTableScan;
import org.imsi.queryEREngine.apache.calcite.schema.QueryableTable;
import org.imsi.queryEREngine.apache.calcite.schema.Table;
import org.imsi.queryEREngine.apache.calcite.tools.RelBuilderFactory;

/** Planner rule that converts a
 * {@link org.imsi.queryEREngine.apache.calcite.rel.logical.LogicalTableFunctionScan}
 * relational expression
 * {@link EnumerableConvention enumerable calling convention}. */
public class EnumerableTableScanRule extends ConverterRule {

	@Deprecated // to be removed before 2.0
	public EnumerableTableScanRule() {
		this(RelFactories.LOGICAL_BUILDER);
	}

	/**
	 * Creates an EnumerableTableScanRule.
	 *
	 * @param relBuilderFactory Builder for relational expressions
	 */
	public EnumerableTableScanRule(RelBuilderFactory relBuilderFactory) {
		super(LogicalTableScan.class,
				(Predicate<LogicalTableScan>) r -> EnumerableTableScan.canHandle(r.getTable()),
				Convention.NONE, EnumerableConvention.INSTANCE, relBuilderFactory,
				"EnumerableTableScanRule");
	}

	@Override public RelNode convert(RelNode rel) {
		LogicalTableScan scan = (LogicalTableScan) rel;
		final RelOptTable relOptTable = scan.getTable();
		final Table table = relOptTable.unwrap(Table.class);
		// The QueryableTable can only be implemented as ENUMERABLE convention,
		// but some test QueryableTables do not really implement the expressions,
		// just skips the QueryableTable#getExpression invocation and returns early.
		if (table instanceof QueryableTable || relOptTable.getExpression(Object.class) != null) {
			return EnumerableTableScan.create(scan.getCluster(), relOptTable);
		}

		return null;
	}
}
