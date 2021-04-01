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
package org.imsi.queryEREngine.apache.calcite.rel;

import org.imsi.queryEREngine.apache.calcite.rel.core.TableFunctionScan;
import org.imsi.queryEREngine.apache.calcite.rel.core.TableScan;
import org.imsi.queryEREngine.apache.calcite.rel.logical.LogicalAggregate;
import org.imsi.queryEREngine.apache.calcite.rel.logical.LogicalCorrelate;
import org.imsi.queryEREngine.apache.calcite.rel.logical.LogicalExchange;
import org.imsi.queryEREngine.apache.calcite.rel.logical.LogicalFilter;
import org.imsi.queryEREngine.apache.calcite.rel.logical.LogicalIntersect;
import org.imsi.queryEREngine.apache.calcite.rel.logical.LogicalJoin;
import org.imsi.queryEREngine.apache.calcite.rel.logical.LogicalMatch;
import org.imsi.queryEREngine.apache.calcite.rel.logical.LogicalMinus;
import org.imsi.queryEREngine.apache.calcite.rel.logical.LogicalProject;
import org.imsi.queryEREngine.apache.calcite.rel.logical.LogicalSort;
import org.imsi.queryEREngine.apache.calcite.rel.logical.LogicalTableModify;
import org.imsi.queryEREngine.apache.calcite.rel.logical.LogicalUnion;
import org.imsi.queryEREngine.apache.calcite.rel.logical.LogicalValues;

/**
 * Visitor that has methods for the common logical relational expressions.
 */
public interface RelShuttle {
	RelNode visit(TableScan scan);

	RelNode visit(TableFunctionScan scan);

	RelNode visit(LogicalValues values);

	RelNode visit(LogicalFilter filter);

	RelNode visit(LogicalProject project);

	RelNode visit(LogicalJoin join);

	RelNode visit(LogicalCorrelate correlate);

	RelNode visit(LogicalUnion union);

	RelNode visit(LogicalIntersect intersect);

	RelNode visit(LogicalMinus minus);

	RelNode visit(LogicalAggregate aggregate);

	RelNode visit(LogicalMatch match);

	RelNode visit(LogicalSort sort);

	RelNode visit(LogicalExchange exchange);

	RelNode visit(LogicalTableModify modify);

	RelNode visit(RelNode other);
}
