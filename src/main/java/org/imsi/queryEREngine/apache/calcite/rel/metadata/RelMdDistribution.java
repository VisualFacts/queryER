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
package org.imsi.queryEREngine.apache.calcite.rel.metadata;

import java.util.List;

import org.imsi.queryEREngine.apache.calcite.plan.RelOptTable;
import org.imsi.queryEREngine.apache.calcite.plan.hep.HepRelVertex;
import org.imsi.queryEREngine.apache.calcite.rel.BiRel;
import org.imsi.queryEREngine.apache.calcite.rel.RelDistribution;
import org.imsi.queryEREngine.apache.calcite.rel.RelDistributions;
import org.imsi.queryEREngine.apache.calcite.rel.RelNode;
import org.imsi.queryEREngine.apache.calcite.rel.SingleRel;
import org.imsi.queryEREngine.apache.calcite.rel.core.Exchange;
import org.imsi.queryEREngine.apache.calcite.rel.core.Filter;
import org.imsi.queryEREngine.apache.calcite.rel.core.Project;
import org.imsi.queryEREngine.apache.calcite.rel.core.SetOp;
import org.imsi.queryEREngine.apache.calcite.rel.core.Snapshot;
import org.imsi.queryEREngine.apache.calcite.rel.core.Sort;
import org.imsi.queryEREngine.apache.calcite.rel.core.TableModify;
import org.imsi.queryEREngine.apache.calcite.rel.core.TableScan;
import org.imsi.queryEREngine.apache.calcite.rel.core.Values;
import org.imsi.queryEREngine.apache.calcite.rel.type.RelDataType;
import org.imsi.queryEREngine.apache.calcite.rex.RexLiteral;
import org.imsi.queryEREngine.apache.calcite.rex.RexNode;
import org.imsi.queryEREngine.apache.calcite.rex.RexProgram;
import org.imsi.queryEREngine.apache.calcite.util.BuiltInMethod;
import org.imsi.queryEREngine.apache.calcite.util.mapping.Mappings;

import com.google.common.collect.ImmutableList;

/**
 * RelMdCollation supplies a default implementation of
 * {@link RelMetadataQuery#distribution}
 * for the standard logical algebra.
 */
public class RelMdDistribution
implements MetadataHandler<BuiltInMetadata.Distribution> {
	public static final RelMetadataProvider SOURCE =
			ReflectiveRelMetadataProvider.reflectiveSource(
					BuiltInMethod.DISTRIBUTION.method, new RelMdDistribution());

	//~ Constructors -----------------------------------------------------------

	private RelMdDistribution() {}

	//~ Methods ----------------------------------------------------------------

	@Override
	public MetadataDef<BuiltInMetadata.Distribution> getDef() {
		return BuiltInMetadata.Distribution.DEF;
	}

	/** Fallback method to deduce distribution for any relational expression not
	 * handled by a more specific method.
	 *
	 * @param rel Relational expression
	 * @return Relational expression's distribution
	 */
	public RelDistribution distribution(RelNode rel, RelMetadataQuery mq) {
		return RelDistributions.SINGLETON;
	}

	public RelDistribution distribution(SingleRel rel, RelMetadataQuery mq) {
		return mq.distribution(rel.getInput());
	}

	public RelDistribution distribution(BiRel rel, RelMetadataQuery mq) {
		return mq.distribution(rel.getLeft());
	}

	public RelDistribution distribution(SetOp rel, RelMetadataQuery mq) {
		return mq.distribution(rel.getInputs().get(0));
	}

	public RelDistribution distribution(TableModify rel, RelMetadataQuery mq) {
		return mq.distribution(rel.getInput());
	}

	public RelDistribution distribution(TableScan scan, RelMetadataQuery mq) {
		return table(scan.getTable());
	}

	public RelDistribution distribution(Project project, RelMetadataQuery mq) {
		return project(mq, project.getInput(), project.getProjects());
	}

	public RelDistribution distribution(Values values, RelMetadataQuery mq) {
		return values(values.getRowType(), values.getTuples());
	}

	public RelDistribution distribution(Exchange exchange, RelMetadataQuery mq) {
		return exchange(exchange.distribution);
	}

	public RelDistribution distribution(HepRelVertex rel, RelMetadataQuery mq) {
		return mq.distribution(rel.getCurrentRel());
	}

	// Helper methods

	/** Helper method to determine a
	 * {@link TableScan}'s distribution. */
	public static RelDistribution table(RelOptTable table) {
		return table.getDistribution();
	}

	/** Helper method to determine a
	 * {@link Snapshot}'s distribution. */
	public static RelDistribution snapshot(RelMetadataQuery mq, RelNode input) {
		return mq.distribution(input);
	}

	/** Helper method to determine a
	 * {@link Sort}'s distribution. */
	public static RelDistribution sort(RelMetadataQuery mq, RelNode input) {
		return mq.distribution(input);
	}

	/** Helper method to determine a
	 * {@link Filter}'s distribution. */
	public static RelDistribution filter(RelMetadataQuery mq, RelNode input) {
		return mq.distribution(input);
	}

	/** Helper method to determine a
	 * limit's distribution. */
	public static RelDistribution limit(RelMetadataQuery mq, RelNode input) {
		return mq.distribution(input);
	}

	/** Helper method to determine a
	 * {@link org.imsi.queryEREngine.apache.calcite.rel.core.Calc}'s distribution. */
	public static RelDistribution calc(RelMetadataQuery mq, RelNode input,
			RexProgram program) {
		assert program.getCondition() != null || !program.getProjectList().isEmpty();
		final RelDistribution inputDistribution = mq.distribution(input);
		if (!program.getProjectList().isEmpty()) {
			final Mappings.TargetMapping mapping = program.getPartialMapping(
					input.getRowType().getFieldCount());
			return inputDistribution.apply(mapping);
		}
		return inputDistribution;
	}

	/** Helper method to determine a {@link Project}'s distribution. */
	public static RelDistribution project(RelMetadataQuery mq, RelNode input,
			List<? extends RexNode> projects) {
		final RelDistribution inputDistribution = mq.distribution(input);
		final Mappings.TargetMapping mapping =
				Project.getPartialMapping(input.getRowType().getFieldCount(),
						projects);
		return inputDistribution.apply(mapping);
	}

	/** Helper method to determine a
	 * {@link Values}'s distribution. */
	public static RelDistribution values(RelDataType rowType,
			ImmutableList<ImmutableList<RexLiteral>> tuples) {
		return RelDistributions.BROADCAST_DISTRIBUTED;
	}

	/** Helper method to determine an
	 * {@link Exchange}'s
	 * or {@link org.imsi.queryEREngine.apache.calcite.rel.core.SortExchange}'s distribution. */
	public static RelDistribution exchange(RelDistribution distribution) {
		return distribution;
	}
}
