package org.imsi.queryEREngine.imsi.calcite.rel.core;

import java.util.List;
import org.imsi.queryEREngine.apache.calcite.plan.RelOptCluster;
import org.imsi.queryEREngine.apache.calcite.plan.RelOptCost;
import org.imsi.queryEREngine.apache.calcite.plan.RelOptPlanner;
import org.imsi.queryEREngine.apache.calcite.plan.RelOptTable;
import org.imsi.queryEREngine.apache.calcite.plan.RelTraitSet;
import org.imsi.queryEREngine.apache.calcite.rel.RelNode;
import org.imsi.queryEREngine.apache.calcite.rel.RelWriter;
import org.imsi.queryEREngine.apache.calcite.rel.SingleRel;
import org.imsi.queryEREngine.apache.calcite.rel.metadata.RelMetadataQuery;
import org.imsi.queryEREngine.apache.calcite.rex.RexNode;
import org.imsi.queryEREngine.apache.calcite.util.Source;
import org.imsi.queryEREngine.imsi.calcite.adapter.enumerable.csv.CsvFieldType;

/**
 * 
 * @author bstam
 * This is the base Class of all Deduplicate relational operators physical or logical.
 * For calcite we need to first create a base class that extends a RelNode class and then
 * extend this class with whatever we want.
 */
public abstract class Deduplicate extends SingleRel {


	protected final RelOptTable table;
	protected final Integer key;
	protected final Source source;
	protected final List<CsvFieldType> fieldTypes;
	protected final List<RexNode> conjuctions;
	protected final RelOptTable blockIndex;
	protected Double comparisons;
	
	protected Deduplicate(
			RelOptCluster cluster,
			RelTraitSet traitSet,
			RelNode input,
			RelOptTable table,
			RelOptTable blockIndex,
			List<RexNode> conjuctions,
			Integer key,
			Source source,
			List<CsvFieldType> fieldTypes,
			Double comparisons){
		super(cluster, traitSet, input);
		this.table = table;
		this.blockIndex = blockIndex;
		this.key = key;
		this.source = source;
		this.conjuctions = conjuctions;
		this.fieldTypes = fieldTypes;
		this.comparisons = comparisons;
		if (table.getRelOptSchema() != null) {
			cluster.getPlanner().registerSchema(table.getRelOptSchema());
		}
	}

	@Override public RelOptCost computeSelfCost(RelOptPlanner planner,
			RelMetadataQuery mq) {
		RelOptCost cost = null;
		
		cost =  super.computeSelfCost(planner, mq)
				.multiplyBy((fieldTypes.size() + 2D)
						/ (table.getRowType().getFieldCount() + 2D));
		return cost;
	}
	
	
	@Override public final RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
		return copy(traitSet, inputs.get(0));
	}

	public abstract RelNode copy(RelTraitSet traitSet, RelNode relNode);

	@Override public RelWriter explainTerms(RelWriter pw) {
		return super.explainTerms(pw)
				.item("tables", table.getQualifiedName()).item("key", this.key);
	}

	public RelOptTable getRelTable() {
		return this.table;
	}


	public Integer getKey() {
		return this.key;
	}


	public Source getSource() {
		return source;
	}

	public List<CsvFieldType> getFieldTypes() {
		return fieldTypes;
	}
	
	public List<RexNode> getConjuctions() {
		return this.conjuctions;
	}
	
	public Double calculateComparisons() {
		if(comparisons == null)
			comparisons = blockIndex.getComparisons(conjuctions, table.getQualifiedName().get(1));
		return comparisons;
	}
	public Double getComparisons() {		
	
			
		return comparisons;
	}

	public RelOptTable getBlockIndex() {
		return blockIndex;
	}
}
