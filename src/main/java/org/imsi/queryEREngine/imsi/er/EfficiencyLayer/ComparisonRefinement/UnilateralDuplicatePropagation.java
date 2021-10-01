package org.imsi.queryEREngine.imsi.er.EfficiencyLayer.ComparisonRefinement;

import org.imsi.queryEREngine.imsi.er.DataStructures.Comparison;
import org.imsi.queryEREngine.imsi.er.DataStructures.IdDuplicates;
import java.util.HashSet;
import java.util.Set;

public class UnilateralDuplicatePropagation extends AbstractDuplicatePropagation {
 
    /**
	 * 
	 */
	private static final long serialVersionUID = -8536355345693699556L;
	private Set<IdDuplicates> detectedDuplicates;
    
    public UnilateralDuplicatePropagation (Set<IdDuplicates> matches) {
        super(matches);
        detectedDuplicates = new HashSet<IdDuplicates>(2*matches.size());
    }
    
    public UnilateralDuplicatePropagation (String groundTruthPath) {
        super(groundTruthPath);
        detectedDuplicates = new HashSet<IdDuplicates>(2*duplicates.size());
    }
    
    @Override
    public int getNoOfDuplicates() {
        return detectedDuplicates.size();
    }
    
    @Override
    public boolean isSuperfluous(Comparison comparison) {
        final IdDuplicates duplicatePair1 = new IdDuplicates(comparison.getEntityId1(), comparison.getEntityId2());
        final IdDuplicates duplicatePair2 = new IdDuplicates(comparison.getEntityId2(), comparison.getEntityId1());
        if (detectedDuplicates.contains(duplicatePair1) || 
                detectedDuplicates.contains(duplicatePair2)) {
            return true;
        }
        
        if (duplicates.contains(duplicatePair1) || 
                duplicates.contains(duplicatePair2)) {
        	
            if (comparison.getEntityId1() < comparison.getEntityId2()) {
                detectedDuplicates.add(new IdDuplicates(comparison.getEntityId1(), comparison.getEntityId2()));
            } else {
                detectedDuplicates.add(new IdDuplicates(comparison.getEntityId2(), comparison.getEntityId1()));
            }
        }
                    
        return false;
    }

    @Override
    public void resetDuplicates() {
        detectedDuplicates = new HashSet<IdDuplicates>();
    }
}