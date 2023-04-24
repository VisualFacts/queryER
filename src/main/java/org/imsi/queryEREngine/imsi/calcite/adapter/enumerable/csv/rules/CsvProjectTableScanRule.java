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
package org.imsi.queryEREngine.imsi.calcite.adapter.enumerable.csv.rules;

import java.util.List;

import org.imsi.queryEREngine.apache.calcite.plan.RelOptRule;
import org.imsi.queryEREngine.apache.calcite.plan.RelOptRuleCall;
import org.imsi.queryEREngine.apache.calcite.rel.core.RelFactories;
import org.imsi.queryEREngine.apache.calcite.rel.logical.LogicalProject;
import org.imsi.queryEREngine.apache.calcite.rex.RexInputRef;
import org.imsi.queryEREngine.apache.calcite.rex.RexNode;
import org.imsi.queryEREngine.apache.calcite.tools.RelBuilderFactory;
import org.imsi.queryEREngine.imsi.calcite.adapter.enumerable.csv.CsvTableScan;
import org.imsi.queryEREngine.imsi.er.KDebug;

/**
 * Planner rule that projects from a {@link CsvTableScan} scan just the columns
 * needed to satisfy a projection. If the projection's expressions are trivial,
 * the projection is removed.
 */
public class CsvProjectTableScanRule extends RelOptRule {
	public static final CsvProjectTableScanRule INSTANCE =
			new CsvProjectTableScanRule(RelFactories.LOGICAL_BUILDER);

	/**
	 * Creates a CsvProjectTableScanRule.
	 *
	 * @param relBuilderFactory Builder for relational expressions
	 */
	public CsvProjectTableScanRule(RelBuilderFactory relBuilderFactory) {
		super(
				operand(LogicalProject.class,
						operand(CsvTableScan.class, none())),
				relBuilderFactory,
				"CsvProjectTableScanRule");
	}

	@Override public void onMatch(RelOptRuleCall call) {
		final LogicalProject project = call.rel(0);
		final CsvTableScan scan = call.rel(1);
		Integer[] fields = getProjectFields(project.getProjects());

		if (fields == null) {
			// Project contains expressions more complex than just field references.
			return;
		}
		call.transformTo(
				new CsvTableScan(
						scan.getCluster(),
						scan.getTable(),
						scan.csvTable,
						fields
						));
	}

	private Integer[] getProjectFields(List<RexNode> exps) {
		final Integer[] fields = new Integer[exps.size()];
		for (int i = 0; i < exps.size(); i++) {
			final RexNode exp = exps.get(i);
			if (exp instanceof RexInputRef) {
				fields[i] = ((RexInputRef) exp).getIndex();
			} else {
				return null; // not a simple projection
			}
		}

		return fields;
	}

	//For debugging
	public static String getCallerCallerClassName() {
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
}
// End CsvProjectTableScanRule.java
