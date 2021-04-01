package org.imsi.queryEREngine.imsi.er.Comparators;

import org.imsi.queryEREngine.imsi.er.DataStructures.Comparison;
import java.util.Comparator;

public class ComparisonUtilityComparator implements Comparator<Comparison> {

    @Override
    public int compare(Comparison o1, Comparison o2) {
        double test = o1.getUtilityMeasure()-o2.getUtilityMeasure(); 
        if (0 < test) {
            return -1;
        }

        if (test < 0) {
            return 1;
        }

        return 0;
    }
    
}
