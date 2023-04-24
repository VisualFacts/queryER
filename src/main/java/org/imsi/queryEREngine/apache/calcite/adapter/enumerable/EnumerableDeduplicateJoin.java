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

import java.util.List;
import java.util.Set;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.imsi.queryEREngine.apache.calcite.plan.RelOptCluster;
import org.imsi.queryEREngine.apache.calcite.plan.RelTraitSet;
import org.imsi.queryEREngine.apache.calcite.rel.RelCollationTraitDef;
import org.imsi.queryEREngine.apache.calcite.rel.RelNode;
import org.imsi.queryEREngine.apache.calcite.rel.core.CorrelationId;
import org.imsi.queryEREngine.apache.calcite.rel.core.Join;
import org.imsi.queryEREngine.apache.calcite.rel.core.JoinRelType;
import org.imsi.queryEREngine.apache.calcite.rel.metadata.RelMdCollation;
import org.imsi.queryEREngine.apache.calcite.rel.metadata.RelMetadataQuery;
import org.imsi.queryEREngine.apache.calcite.rex.RexNode;
import org.imsi.queryEREngine.apache.calcite.rex.RexUtil;
import org.imsi.queryEREngine.apache.calcite.util.Source;
import org.imsi.queryEREngine.apache.calcite.util.Util;
import org.imsi.queryEREngine.imsi.calcite.adapter.enumerable.csv.CsvFieldType;
import org.imsi.queryEREngine.imsi.calcite.util.NewBuiltInMethod;
import com.google.common.collect.ImmutableList;


/** Implementation of {@link org.imsi.queryEREngine.apache.calcite.rel.core.Deduplicate} in
 * {@link org.imsi.queryEREngine.apache.calcite.adapter.enumerable.EnumerableConvention enumerable calling convention}. */
public class EnumerableDeduplicateJoin extends Join implements EnumerableRel {

	/**
	 * 
	 * @param cluster
	 * @param traits 
	 * @param left 
	 * @param right
	 * @param condition
	 * @param variablesSet
	 * @param joinType
	 * @param keyLeft
	 * @param keyRight
	 * @param tableNameLeft
	 * @param tableNameRight
	 * @param fieldLeft
	 * @param fieldRight
	 */
	protected EnumerableDeduplicateJoin(
			RelOptCluster cluster,
			RelTraitSet traits,
			RelNode left,
			RelNode right,
			RexNode condition,
			Set<CorrelationId> variablesSet,
			JoinRelType joinType,
			Source sourceLeft,
			Source sourceRight,
			List<CsvFieldType> fieldTypesLeft,
			List<CsvFieldType> fieldTypesRight,
			Integer keyLeft,
			Integer keyRight,
			String tableNameLeft,
			String tableNameRight,
			Integer fieldLeft,
			Integer fieldRight,
			Boolean isDirtyJoin
			){
		super(
				cluster,
				traits,
				ImmutableList.of(),
				left,
				right,
				condition,
				variablesSet,
				joinType);
		this.traitSet =
				cluster.traitSet().replace(EnumerableConvention.INSTANCE);
		this.setSourceLeft(sourceLeft);
		this.setSourceRight(sourceRight);
		this.setKeyLeft(keyLeft);
		this.setKeyRight(keyRight);
		this.setTableNameLeft(tableNameLeft);
		this.setTableNameRight(tableNameRight);
		this.setFieldLeft(fieldLeft);
		this.setFieldRight(fieldRight);
		this.setDirtyJoin(isDirtyJoin);
		this.setFieldTypesLeft(fieldTypesLeft);
		this.setFieldTypesRight(fieldTypesRight);

	}


	public static RelNode create(
			RelNode left,
			RelNode right,
			RexNode condition,
			Set<CorrelationId> variablesSet,
			JoinRelType joinType,
			Source sourceLeft,
			Source sourceRight,
			List<CsvFieldType> fieldTypesLeft,
			List<CsvFieldType> fieldTypesRight,
			Integer keyLeft,
			Integer keyRight,
			String tableNameLeft,
			String tableNameRight,
			Integer fieldLeft,
			Integer fieldRight,
			Boolean isDirtyJoin) {
		final RelOptCluster cluster = left.getCluster();
		final RelMetadataQuery mq = cluster.getMetadataQuery();
		final RelTraitSet traitSet =
				cluster.traitSetOf(EnumerableConvention.INSTANCE)
				.replaceIfs(RelCollationTraitDef.INSTANCE,
						() -> RelMdCollation.enumerableHashJoin(mq, left, right, joinType));
		return new EnumerableDeduplicateJoin(cluster, traitSet, left, right, condition,
				variablesSet, joinType, sourceLeft, sourceRight, fieldTypesLeft, fieldTypesRight, 
				keyLeft, keyRight, tableNameLeft,
				 tableNameRight, fieldLeft, fieldRight, isDirtyJoin);
	}
	


	@Override public  EnumerableDeduplicateJoin copy(RelTraitSet traitSet, RexNode condition,
			RelNode left, RelNode right, JoinRelType joinType, Source sourceLeft, Source sourceRight,
			List<CsvFieldType> fieldTypesLeft, List<CsvFieldType> fieldTypesRight,
			boolean semiJoinDone, Integer keyRight, Integer keyLeft,
			String tableNameLeft,
			String tableNameRight,
			Integer fieldLeft,
			Integer fieldRight,
			Boolean isDirtyJoin) {
		return new EnumerableDeduplicateJoin(getCluster(), traitSet, left, right,
				condition, variablesSet, joinType, sourceLeft, sourceRight, fieldTypesLeft, fieldTypesRight, 
				keyRight, keyLeft, tableNameLeft, tableNameRight, fieldLeft, fieldRight, isDirtyJoin);
	
	}


	@Override
	public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
		switch (joinType) {
			case DIRTY_RIGHT:
				return implementHashJoinDirtyRight(implementor, pref);
			case DIRTY_LEFT:
				return implementHashJoinDirtyLeft(implementor, pref);			
			case CLEAN:
				return implementHashJoinClean(implementor, pref);
			default:
				return null;
		}
	}


	@Override
	public Join copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, JoinRelType joinType,
			boolean semiJoinDone) {
		// TODO Auto-generated method stub
		return new EnumerableDeduplicateJoin(getCluster(), traitSet, left, right,
				condition, variablesSet, joinType, this.getSourceLeft(), this.getSourceRight(),
				this.getFieldTypesLeft(), this.getFieldTypesRight(),
				this.getKeyLeft(), this.getKeyRight(), this.getTableNameLeft(), this.getTableNameRight(),
				this.getFieldLeft(), this.getFieldRight(), this.isDirtyJoin());
	}

	/**
	 * 
	 * @param implementor
	 * @param pref
	 * @return Result
	 * 
	 * Implementation of the rightDirtyJoin, by passing to the function the data needed to potentially
	 * deduplicate the right table.
	 */
	private Result implementHashJoinDirtyRight(EnumerableRelImplementor implementor, Prefer pref) {
		// TODO Auto-generated method stub
		final BlockBuilder builder = new BlockBuilder();

		final Result leftResult =
				implementor.visitChild(this, 0, (EnumerableRel) left, pref);
		Expression leftExpression =
				builder.append(
						"left", leftResult.block);
	
		final Result rightResult =
				implementor.visitChild(this, 1, (EnumerableRel) right, pref);
		
		Expression rightExpression =
				builder.append(
						"right", rightResult.block);
		
		PhysType physType = PhysTypeImpl.of(
						implementor.getTypeFactory(), getRowType(), pref.prefer(leftResult.format));

		final PhysType keyPhysType =
				leftResult.physType.project(
						joinInfo.leftKeys, JavaRowFormat.LIST);
		Expression predicate = Expressions.constant(null);
		if (!joinInfo.nonEquiConditions.isEmpty()) {
			RexNode nonEquiCondition = RexUtil.composeConjunction(
					getCluster().getRexBuilder(), joinInfo.nonEquiConditions, true);
			if (nonEquiCondition != null) {
				predicate = EnumUtils.generatePredicate(implementor, getCluster().getRexBuilder(),
						left, right, leftResult.physType, rightResult.physType, nonEquiCondition);
			}
		}
		builder.add(
				Expressions.return_(null, Expressions.call(
						NewBuiltInMethod.HASH_JOIN_DIRTY_RIGHT.method,
						Expressions.list(
								leftExpression,
								rightExpression,
								leftResult.physType.generateAccessor(joinInfo.leftKeys),
								rightResult.physType.generateAccessor(joinInfo.rightKeys),
								EnumUtils.joinSelector(joinType,
										physType,
										ImmutableList.of(
												leftResult.physType, rightResult.physType)),
								Expressions.constant(this.getSourceRight().toString()),
								Expressions.constant(this.getFieldTypesRight()),
								Expressions.constant(this.getKeyRight()),
								Expressions.constant(this.getTableNameRight()),
								Expressions.constant(this.getFieldRight()))
						.append(
								Util.first(keyPhysType.comparer(),
										Expressions.constant(null)))
						.append(
								Expressions.constant(joinType.generatesNullsOnLeft()))
						.append(
								Expressions.constant(
										joinType.generatesNullsOnRight()))
						.append(predicate))));
		//System.out.println(builder.toBlock());

		return implementor.result(
				physType, builder.toBlock());
	}
	
	
	/**
	 * 
	 * @param implementor
	 * @param pref
	 * @return Result
	 * 
	 * Implementation of the leftDirtyJoin, by passing to the function the data needed to potentially
	 * deduplicate the left table.
	 */
	private Result implementHashJoinDirtyLeft(EnumerableRelImplementor implementor, Prefer pref) {
		// TODO Auto-generated method stub
		final BlockBuilder builder = new BlockBuilder();

		final Result leftResult =
				implementor.visitChild(this, 0, (EnumerableRel) left, pref);
		Expression leftExpression =
				builder.append(
						"left", leftResult.block);
	
		final Result rightResult =
				implementor.visitChild(this, 1, (EnumerableRel) right, pref);
		
		Expression rightExpression =
				builder.append(
						"right", rightResult.block);
		
		PhysType physType = PhysTypeImpl.of(
						implementor.getTypeFactory(), getRowType(), pref.prefer(leftResult.format));

		final PhysType keyPhysType =
				leftResult.physType.project(
						joinInfo.leftKeys, JavaRowFormat.LIST);
		Expression predicate = Expressions.constant(null);
		if (!joinInfo.nonEquiConditions.isEmpty()) {
			RexNode nonEquiCondition = RexUtil.composeConjunction(
					getCluster().getRexBuilder(), joinInfo.nonEquiConditions, true);
			if (nonEquiCondition != null) {
				predicate = EnumUtils.generatePredicate(implementor, getCluster().getRexBuilder(),
						left, right, leftResult.physType, rightResult.physType, nonEquiCondition);
			}
		}
		builder.add(
				Expressions.return_(null, Expressions.call(
						NewBuiltInMethod.HASH_JOIN_DIRTY_LEFT.method,
						Expressions.list(
								leftExpression,
								rightExpression,
								leftResult.physType.generateAccessor(joinInfo.leftKeys),
								rightResult.physType.generateAccessor(joinInfo.rightKeys),
								EnumUtils.joinSelector(joinType,
										physType,
										ImmutableList.of(
												leftResult.physType, rightResult.physType)),
								Expressions.constant(this.getSourceLeft().toString()),
								Expressions.constant(this.getFieldTypesLeft()),
								Expressions.constant(this.getKeyLeft()),
								Expressions.constant(this.getTableNameLeft()),
								Expressions.constant(this.getFieldLeft()))
						.append(
								Util.first(keyPhysType.comparer(),
										Expressions.constant(null)))
						.append(
								Expressions.constant(joinType.generatesNullsOnLeft()))
						.append(
								Expressions.constant(
										joinType.generatesNullsOnRight()))
						.append(predicate))));
		//System.out.println(builder.toBlock());

		return implementor.result(
				physType, builder.toBlock());
	}
	
	
	
	/**
	 * 
	 * @param implementor
	 * @param pref
	 * @return Result
	 * 
	 * Implementation of the Clean-Clean Join, by passing two entity resolved tuples.
	 */
	private Result implementHashJoinClean(EnumerableRelImplementor implementor, Prefer pref) {
		// TODO Auto-generated method stub
		final BlockBuilder builder = new BlockBuilder();

		final Result leftResult =
				implementor.visitChild(this, 0, (EnumerableRel) left, pref);
		Expression leftExpression =
				builder.append(
						"left", leftResult.block);
	
		final Result rightResult =
				implementor.visitChild(this, 1, (EnumerableRel) right, pref);
		
		Expression rightExpression =
				builder.append(
						"right", rightResult.block);
		
		PhysType physType = PhysTypeImpl.of(
						implementor.getTypeFactory(), getRowType(), pref.prefer(leftResult.format));

		final PhysType keyPhysType =
				leftResult.physType.project(
						joinInfo.leftKeys, JavaRowFormat.LIST);
		Expression predicate = Expressions.constant(null);
		if (!joinInfo.nonEquiConditions.isEmpty()) {
			RexNode nonEquiCondition = RexUtil.composeConjunction(
					getCluster().getRexBuilder(), joinInfo.nonEquiConditions, true);
			if (nonEquiCondition != null) {
				predicate = EnumUtils.generatePredicate(implementor, getCluster().getRexBuilder(),
						left, right, leftResult.physType, rightResult.physType, nonEquiCondition);
			}
		}
		builder.add(
				Expressions.return_(null, Expressions.call(
						NewBuiltInMethod.HASH_JOIN_CLEAN.method,
						Expressions.list(
								leftExpression,
								rightExpression,
								leftResult.physType.generateAccessor(joinInfo.leftKeys),
								rightResult.physType.generateAccessor(joinInfo.rightKeys),
								EnumUtils.joinSelector(joinType,
										physType,
										ImmutableList.of(
												leftResult.physType, rightResult.physType)))
						.append(
								Util.first(keyPhysType.comparer(),
										Expressions.constant(null)))
						.append(
								Expressions.constant(joinType.generatesNullsOnLeft()))
						.append(
								Expressions.constant(
										joinType.generatesNullsOnRight()))
						.append(predicate))));
		//System.out.println(builder.toBlock());

		return implementor.result(
				physType, builder.toBlock());
	}
	
	
}



