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
package org.imsi.queryEREngine.apache.calcite.prepare;

import java.util.ArrayList;
import java.util.List;

import org.imsi.queryEREngine.apache.calcite.adapter.enumerable.EnumerableRel;
import org.imsi.queryEREngine.apache.calcite.config.CalciteSystemProperty;
import org.imsi.queryEREngine.apache.calcite.interpreter.BindableConvention;
import org.imsi.queryEREngine.apache.calcite.jdbc.CalcitePrepare;
import org.imsi.queryEREngine.apache.calcite.jdbc.CalciteSchema;
import org.imsi.queryEREngine.apache.calcite.plan.RelOptCluster;
import org.imsi.queryEREngine.apache.calcite.plan.RelOptMaterialization;
import org.imsi.queryEREngine.apache.calcite.plan.RelOptTable;
import org.imsi.queryEREngine.apache.calcite.plan.RelOptUtil;
import org.imsi.queryEREngine.apache.calcite.rel.RelNode;
import org.imsi.queryEREngine.apache.calcite.rel.RelRoot;
import org.imsi.queryEREngine.apache.calcite.rel.RelShuttle;
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
import org.imsi.queryEREngine.apache.calcite.schema.Schemas;
import org.imsi.queryEREngine.apache.calcite.schema.Table;
import org.imsi.queryEREngine.apache.calcite.schema.impl.StarTable;
import org.imsi.queryEREngine.apache.calcite.sql.SqlNode;
import org.imsi.queryEREngine.apache.calcite.sql.parser.SqlParseException;
import org.imsi.queryEREngine.apache.calcite.sql.parser.SqlParser;
import org.imsi.queryEREngine.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.imsi.queryEREngine.apache.calcite.sql2rel.SqlToRelConverter;

import com.google.common.collect.ImmutableList;

/**
 * Context for populating a {@link Prepare.Materialization}.
 */
class CalciteMaterializer extends CalcitePrepareImpl.CalcitePreparingStmt {
	CalciteMaterializer(CalcitePrepareImpl prepare,
			CalcitePrepare.Context context,
			CatalogReader catalogReader, CalciteSchema schema,
			RelOptCluster cluster, SqlRexConvertletTable convertletTable) {
		super(prepare, context, catalogReader, catalogReader.getTypeFactory(),
				schema, EnumerableRel.Prefer.ANY, cluster, BindableConvention.INSTANCE,
				convertletTable);
	}

	/** Populates a materialization record, converting a table path
	 * (essentially a list of strings, like ["hr", "sales"]) into a table object
	 * that can be used in the planning process. */
	void populate(Materialization materialization) {
		SqlParser parser = SqlParser.create(materialization.sql);
		SqlNode node;
		try {
			node = parser.parseStmt();
		} catch (SqlParseException e) {
			throw new RuntimeException("parse failed", e);
		}
		final SqlToRelConverter.Config config = SqlToRelConverter.configBuilder()
				.withTrimUnusedFields(true).build();
		SqlToRelConverter sqlToRelConverter2 =
				getSqlToRelConverter(getSqlValidator(), catalogReader, config);

		RelRoot root = sqlToRelConverter2.convertQuery(node, true, true);
		materialization.queryRel = trimUnusedFields(root).rel;

		// Identify and substitute a StarTable in queryRel.
		//
		// It is possible that no StarTables match. That is OK, but the
		// materialization patterns that are recognized will not be as rich.
		//
		// It is possible that more than one StarTable matches. TBD: should we
		// take the best (whatever that means), or all of them?
		useStar(schema, materialization);

		RelOptTable table =
				this.catalogReader.getTable(materialization.materializedTable.path());
		materialization.tableRel = sqlToRelConverter2.toRel(table, ImmutableList.of());
	}

	/** Converts a relational expression to use a
	 * {@link StarTable} defined in {@code schema}.
	 * Uses the first star table that fits. */
	private void useStar(CalciteSchema schema, Materialization materialization) {
		for (Callback x : useStar(schema, materialization.queryRel)) {
			// Success -- we found a star table that matches.
			materialization.materialize(x.rel, x.starRelOptTable);
			if (CalciteSystemProperty.DEBUG.value()) {
				System.out.println("Materialization "
						+ materialization.materializedTable + " matched star table "
						+ x.starTable + "; query after re-write: "
						+ RelOptUtil.toString(materialization.queryRel));
			}
		}
	}

	/** Converts a relational expression to use a
	 * {@link org.imsi.queryEREngine.apache.calcite.schema.impl.StarTable} defined in {@code schema}.
	 * Uses the first star table that fits. */
	private Iterable<Callback> useStar(CalciteSchema schema, RelNode queryRel) {
		List<CalciteSchema.TableEntry> starTables =
				Schemas.getStarTables(schema.root());
		if (starTables.isEmpty()) {
			// Don't waste effort converting to leaf-join form.
			return ImmutableList.of();
		}
		final List<Callback> list = new ArrayList<>();
		final RelNode rel2 =
				RelOptMaterialization.toLeafJoinForm(queryRel);
		for (CalciteSchema.TableEntry starTable : starTables) {
			final Table table = starTable.getTable();
			assert table instanceof StarTable;
			RelOptTableImpl starRelOptTable =
					RelOptTableImpl.create(catalogReader, table.getRowType(typeFactory),
							starTable, null, null);
			final RelNode rel3 =
					RelOptMaterialization.tryUseStar(rel2, starRelOptTable);
			if (rel3 != null) {
				list.add(new Callback(rel3, starTable, starRelOptTable));
			}
		}
		return list;
	}

	/** Implementation of {@link RelShuttle} that returns each relational
	 * expression unchanged. It does not visit inputs. */
	static class RelNullShuttle implements RelShuttle {
		@Override
		public RelNode visit(TableScan scan) {
			return scan;
		}
		@Override
		public RelNode visit(TableFunctionScan scan) {
			return scan;
		}
		@Override
		public RelNode visit(LogicalValues values) {
			return values;
		}
		@Override
		public RelNode visit(LogicalFilter filter) {
			return filter;
		}
		@Override
		public RelNode visit(LogicalProject project) {
			return project;
		}
		@Override
		public RelNode visit(LogicalJoin join) {
			return join;
		}
		@Override
		public RelNode visit(LogicalCorrelate correlate) {
			return correlate;
		}
		@Override
		public RelNode visit(LogicalUnion union) {
			return union;
		}
		@Override
		public RelNode visit(LogicalIntersect intersect) {
			return intersect;
		}
		@Override
		public RelNode visit(LogicalMinus minus) {
			return minus;
		}
		@Override
		public RelNode visit(LogicalAggregate aggregate) {
			return aggregate;
		}
		@Override
		public RelNode visit(LogicalMatch match) {
			return match;
		}
		@Override
		public RelNode visit(LogicalSort sort) {
			return sort;
		}
		@Override
		public RelNode visit(LogicalExchange exchange) {
			return exchange;
		}
		@Override
		public RelNode visit(LogicalTableModify modify) {
			return modify;
		}
		@Override
		public RelNode visit(RelNode other) {
			return other;
		}
	}

	/** Called when we discover a star table that matches. */
	static class Callback {
		public final RelNode rel;
		public final CalciteSchema.TableEntry starTable;
		public final RelOptTableImpl starRelOptTable;

		Callback(RelNode rel,
				CalciteSchema.TableEntry starTable,
				RelOptTableImpl starRelOptTable) {
			this.rel = rel;
			this.starTable = starTable;
			this.starRelOptTable = starRelOptTable;
		}
	}
}
