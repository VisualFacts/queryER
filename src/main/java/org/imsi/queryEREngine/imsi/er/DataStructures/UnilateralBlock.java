package org.imsi.queryEREngine.imsi.er.DataStructures;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Set;

public class UnilateralBlock extends AbstractBlock implements Serializable {

    private static final long serialVersionUID = 43532585408538695L;

    protected final int[] entities;
    protected int[] queryEntities= new int[0];

    public UnilateralBlock(int[] entities) {
        super();
        this.entities = entities;
    }

    public UnilateralBlock(int[] entities, Set<Integer> qIds) {
        super();
        this.entities = entities;
        this.queryEntities = Arrays.stream(entities).filter(qIds::contains).toArray();
//		this.entities= Arrays.stream(entities).filter(e -> !qIds.contains(e)).toArray();
    }


    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final UnilateralBlock other = (UnilateralBlock) obj;
        if (!Arrays.equals(this.entities, other.entities)) {
            return false;
        }
        return true;
    }

    public int[] getEntities() {
//		return Stream.of(entities, queryEntities)
//				.flatMap(Stream::of).toArray(int[]::new);

        return entities;
//		return IntStream.concat(Arrays.stream(entities), Arrays.stream(queryEntities)).toArray();
    }

    public int[] getQueryEntities() {
        return queryEntities;
    }

    public void setQueryEntities(int[] queryEntities) {
//		Set<Integer> set = Arrays.stream(queryEntities).boxed().collect(Collectors.toSet());
//		this.queryEntities = Arrays.stream(entities).filter(set::contains).toArray();
//		Arrays.stream(entities).filter(queryEntities::contains).toArray();

        this.queryEntities = queryEntities;
    }

    @Override
    public double getNoOfComparisons() {
        double noOfEntities = entities.length;
        double noOfQueryEntities = 0;
        boolean qE = queryEntities.length > 0;
        if (qE) noOfQueryEntities = queryEntities.length;
//		System.err.println(noOfEntities+"   ###   "+noOfQueryEntities);
        if (qE) return (noOfQueryEntities * noOfEntities) - noOfQueryEntities;

        return noOfEntities * (noOfEntities - 1) / 2;
    }

    @Override
    public double getTotalBlockAssignments() {
        return entities.length;
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 53 * hash + Arrays.hashCode(this.entities);
        return hash;
    }

    @Override
    public void setUtilityMeasure() {
        utilityMeasure = 1.0 / entities.length;
    }
}