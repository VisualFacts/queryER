package org.imsi.queryEREngine.imsi.er.MetaBlocking;

import org.imsi.queryEREngine.imsi.er.DataStructures.AbstractBlock;
import org.imsi.queryEREngine.imsi.er.DataStructures.Comparison;
import org.imsi.queryEREngine.imsi.er.DataStructures.EntityProfile;
import org.imsi.queryEREngine.imsi.er.Utilities.ComparisonIterator;
import org.imsi.queryEREngine.imsi.er.Utilities.MetaBlockingConfiguration.WeightingScheme;
import org.imsi.queryEREngine.imsi.er.Utilities.ProfileComparison;
import java.util.List;

/**
 *
 * @author gap2
 */
public class EdgePruningIntegratedMatching extends EdgePruning {

    private final EntityProfile[] profiles1;
    private final EntityProfile[] profiles2;

    public EdgePruningIntegratedMatching(List<EntityProfile> pr1,
            List<EntityProfile> pr2, WeightingScheme scheme) {
        super("Edge Pruning with integrated matching", scheme);
        profiles1 = pr1.toArray(new EntityProfile[pr1.size()]);
        if (pr2 == null) {
            profiles2 = null;
        } else {
            profiles2 = pr2.toArray(new EntityProfile[pr2.size()]);
        }
    }

    @Override
    protected void filterComparisons(List<AbstractBlock> blocks) {
        boolean cleanCleanER = profiles2 != null;
        for (AbstractBlock block : blocks) {
            ComparisonIterator iterator = block.getComparisonIterator();
            while (iterator.hasNext()) {
                Comparison comparison = iterator.next();
                double weight = getWeight(block.getBlockIndex(), comparison);
                if (weight < averageWeight) {
                    continue;
                }
                if (cleanCleanER) {
                    ProfileComparison.getJaccardSimilarity(profiles1[comparison.getEntityId1()].getAttributes(),
                            profiles2[comparison.getEntityId2()].getAttributes());
                } else {
                    ProfileComparison.getJaccardSimilarity(profiles1[comparison.getEntityId1()].getAttributes(),
                            profiles1[comparison.getEntityId2()].getAttributes());
                }
            }
        }

    }
}
