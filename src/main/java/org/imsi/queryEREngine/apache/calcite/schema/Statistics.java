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
package org.imsi.queryEREngine.apache.calcite.schema;

import java.util.List;

import org.imsi.queryEREngine.apache.calcite.rel.RelCollation;
import org.imsi.queryEREngine.apache.calcite.rel.RelDistribution;
import org.imsi.queryEREngine.apache.calcite.rel.RelDistributionTraitDef;
import org.imsi.queryEREngine.apache.calcite.rel.RelReferentialConstraint;
import org.imsi.queryEREngine.apache.calcite.rex.RexNode;
import org.imsi.queryEREngine.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;

/**
 * Utility functions regarding {@link Statistic}.
 */
public class Statistics {
	private Statistics() {
	}

	/** Returns a {@link Statistic} that knows nothing about a table. */
	public static final Statistic UNKNOWN =
			new Statistic() {
		@Override
		public Double getRowCount() {
			return null;
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
		public Double getComparisons(List<RexNode> paramList, String tableName) {
			return null;
		}
	};

	/** Returns a statistic with a given set of referential constraints. */
	public static Statistic of(final List<RelReferentialConstraint> referentialConstraints) {
		return of(null, ImmutableList.of(),
				referentialConstraints, ImmutableList.of());
	}

	/** Returns a statistic with a given row count and set of unique keys. */
	public static Statistic of(final double rowCount,
			final List<ImmutableBitSet> keys) {
		return of(rowCount, keys, ImmutableList.of(),
				ImmutableList.of());
	}

	/** Returns a statistic with a given row count, set of unique keys,
	 * and collations. */
	public static Statistic of(final double rowCount,
			final List<ImmutableBitSet> keys,
			final List<RelCollation> collations) {
		return of(rowCount, keys, ImmutableList.of(), collations);
	}

	/** Returns a statistic with a given row count, set of unique keys,
	 * referential constraints, and collations. */
	public static Statistic of(final Double rowCount,
			final List<ImmutableBitSet> keys,
			final List<RelReferentialConstraint> referentialConstraints,
			final List<RelCollation> collations) {
		return new Statistic() {
			@Override
			public Double getRowCount() {
				return rowCount;
			}

			@Override
			public boolean isKey(ImmutableBitSet columns) {
				for (ImmutableBitSet key : keys) {
					if (columns.contains(key)) {
						return true;
					}
				}
				return false;
			}

			@Override
			public List<ImmutableBitSet> getKeys() {
				return ImmutableList.copyOf(keys);
			}

			@Override
			public List<RelReferentialConstraint> getReferentialConstraints() {
				return referentialConstraints;
			}

			@Override
			public List<RelCollation> getCollations() {
				return collations;
			}

			@Override
			public RelDistribution getDistribution() {
				return RelDistributionTraitDef.INSTANCE.getDefault();
			}

			@Override
			public Double getComparisons(List<RexNode> paramList, String tableName) {
				// TODO Auto-generated method stub
				return null;
			}
		};
	}
}
