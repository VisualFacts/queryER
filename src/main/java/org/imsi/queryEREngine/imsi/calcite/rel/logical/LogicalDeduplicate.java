package org.imsi.queryEREngine.imsi.calcite.rel.logical;

import java.util.List;

import org.imsi.queryEREngine.apache.calcite.plan.RelOptCluster;
import org.imsi.queryEREngine.apache.calcite.plan.RelOptTable;
import org.imsi.queryEREngine.apache.calcite.plan.RelTraitSet;
import org.imsi.queryEREngine.apache.calcite.rel.RelNode;
import org.imsi.queryEREngine.apache.calcite.rex.RexNode;
import org.imsi.queryEREngine.apache.calcite.util.Source;
import org.imsi.queryEREngine.imsi.calcite.adapter.enumerable.csv.CsvFieldType;
import org.imsi.queryEREngine.imsi.calcite.rel.core.Deduplicate;

/**
 * @author bstam
 * The Deduplicate relational operator apart from the filtered data
 * gets as input a table along with its key, source and fieldTypes.
 * These are the exact inputs of the Scan relational operator and that is because
 * the Deduplication process potentially requires an extra scan during the resolution phase.
 * 
 * 
 */
public class LogicalDeduplicate extends Deduplicate {
	 protected LogicalDeduplicate(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RelOptTable table, RelOptTable blockIndex, List<RexNode> conjuctions, Integer key, Source source, List<CsvFieldType> fieldTypes, Double comparisons) {
		 super(cluster, traitSet, input, table, blockIndex, conjuctions, key, source, fieldTypes, comparisons);
	 }

	 public static RelNode create(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RelOptTable table, RelOptTable blockIndex, List<RexNode> conjuctions, Integer key, Source source, List<CsvFieldType> fieldTypes, Double comparisons) {
		 return new LogicalDeduplicate(cluster, traitSet, input, table, blockIndex, conjuctions, key, source, fieldTypes, comparisons);
	 }

	 @Override
	public RelNode copy(RelTraitSet traitSet, RelNode input) {
		 return new LogicalDeduplicate(getCluster(), traitSet, input, this.table, this.blockIndex, this.conjuctions, this.key, this.source, this.fieldTypes, this.comparisons);
	 }
 }
