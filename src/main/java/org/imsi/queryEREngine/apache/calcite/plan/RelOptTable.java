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
package org.imsi.queryEREngine.apache.calcite.plan;

import java.util.List;

import org.apache.calcite.linq4j.tree.Expression;
import org.imsi.queryEREngine.apache.calcite.rel.RelCollation;
import org.imsi.queryEREngine.apache.calcite.rel.RelDistribution;
import org.imsi.queryEREngine.apache.calcite.rel.RelNode;
import org.imsi.queryEREngine.apache.calcite.rel.RelReferentialConstraint;
import org.imsi.queryEREngine.apache.calcite.rel.RelRoot;
import org.imsi.queryEREngine.apache.calcite.rel.hint.RelHint;
import org.imsi.queryEREngine.apache.calcite.rel.type.RelDataType;
import org.imsi.queryEREngine.apache.calcite.rel.type.RelDataTypeField;
import org.imsi.queryEREngine.apache.calcite.rex.RexNode;
import org.imsi.queryEREngine.apache.calcite.schema.ColumnStrategy;
import org.imsi.queryEREngine.apache.calcite.schema.Wrapper;
import org.imsi.queryEREngine.apache.calcite.util.ImmutableBitSet;

/**
 * Represents a relational dataset in a {@link RelOptSchema}. It has methods to
 * describe and implement itself.
 */
public interface RelOptTable extends Wrapper {
	List<String> getQualifiedName();

	double getRowCount();

	RelDataType getRowType();

	RelOptSchema getRelOptSchema();

	RelNode toRel(ToRelContext paramToRelContext);

	List<RelCollation> getCollationList();

	RelDistribution getDistribution();

	boolean isKey(ImmutableBitSet paramImmutableBitSet);

	List<ImmutableBitSet> getKeys();

	List<RelReferentialConstraint> getReferentialConstraints();

	Expression getExpression(Class paramClass);

	RelOptTable extend(List<RelDataTypeField> paramList);

	List<ColumnStrategy> getColumnStrategies();

	double getComparisons(List<RexNode> paramList, String string);

	public static interface ToRelContext extends ViewExpander {
		RelOptCluster getCluster();

		List<RelHint> getTableHints();
	}

	public static interface ViewExpander {
		RelRoot expandView(RelDataType param1RelDataType, String param1String, List<String> param1List1, List<String> param1List2);
	}
}
