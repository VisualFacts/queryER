//package org.imsi.queryEREngine.imsi.er.EfficiencyLayer.MemoryBased;
//
//import org.imsi.queryEREngine.imsi.er.BlockBuilding.AbstractCanopies;
//import org.imsi.queryEREngine.imsi.er.Comparators.ComparisonWeightComparator;
//import org.imsi.queryEREngine.imsi.er.DataStructures.AbstractBlock;
//import org.imsi.queryEREngine.imsi.er.DataStructures.BilateralBlock;
//import org.imsi.queryEREngine.imsi.er.DataStructures.Comparison;
//import org.imsi.queryEREngine.imsi.er.DataStructures.EntityProfile;
//import org.imsi.queryEREngine.imsi.er.DataStructures.UnilateralBlock;
//import org.imsi.queryEREngine.imsi.er.Utilities.Converter;
//import java.util.ArrayList;
//import java.util.HashSet;
//import java.util.Iterator;
//import java.util.List;
//import java.util.PriorityQueue;
//import java.util.Queue;
//import java.util.Set;
//
//
//
//public class CanopyClusteringNN extends AbstractCanopies {
//
//    private final int n1;
//    private final int n2;
//    private double minimumWeight;
//    private Queue<Comparison> n2NearestEntities;
//    
//    public CanopyClusteringNN(int n1, int n2, List<EntityProfile> profiles1, List<EntityProfile> profiles2) {
//        super(profiles1, profiles2, "In-memory Canopy Clustering with Nearest Neighbors");
//        this.n1 = n1;
//        this.n2 = n2;
//    }
//    
//    private void addComparison(Comparison comparison) {
//        if (comparison.getUtilityMeasure() < minimumWeight) {
//            return;
//        }
//
//        n2NearestEntities.add(comparison);
//        if (n2 < n2NearestEntities.size()) {
//            Comparison lastComparison = (Comparison) n2NearestEntities.poll();
//            minimumWeight = lastComparison.getUtilityMeasure();
//        }
//    }
//    
//    @Override
//    protected List<AbstractBlock> getBilateralBlocks() {
//        int noOfProfiles1 = profiles1.length;
//        final Set<Integer> entityIds1 = new HashSet<Integer>(2 * noOfProfiles1);
//        for (int i = 0; i < noOfProfiles1; i++) {
//            entityIds1.add(i);
//        }
//
//        int noOfProfiles2 = profiles2.length;
//        final Set<Integer> entityIds2 = new HashSet<Integer>(2 * noOfProfiles2);
//        for (int i = 0; i < noOfProfiles2; i++) {
//            entityIds2.add(i);
//        }
//
//        final List<AbstractBlock> blocks = new ArrayList<AbstractBlock>();
//        while (!entityIds1.isEmpty() && !entityIds2.isEmpty()) {
//            // Get first element:
//            Iterator iter1 = entityIds1.iterator();
//            int firstId = (Integer) iter1.next();
//
//            // Remove first element:
//            iter1.remove();
//            entityIds1.remove(firstId);
//
//            // Start a new cluster:
//            final List<Integer> newBlockIds = new ArrayList<Integer>();
//            minimumWeight = Double.MIN_VALUE;
//            n2NearestEntities = new PriorityQueue<Comparison>((int) (2 * n2), new ComparisonWeightComparator());
//
//            // Compare to remaining objects:
//            Iterator iter2 = entityIds2.iterator();
//            while (iter2.hasNext()) {
//                int currentId = (Integer) iter2.next();
//                double jaccardSim = getJaccardSimilarity(profiles1[firstId], profiles2[currentId]);
//
//                Comparison comparison = new Comparison(false, firstId, currentId);
//                comparison.setUtilityMeasure(jaccardSim);
//                addComparison(comparison);
//            }
//
//            for (int i = 0; i < n2 && !n2NearestEntities.isEmpty(); i++) {
//                Comparison lastComparison = (Comparison) n2NearestEntities.poll();
//                newBlockIds.add(lastComparison.getEntityId2());
//                if (i >= (n2 - n1)) {
//                    entityIds2.remove(lastComparison.getEntityId2());
//                }
//            }
//
//            if (!newBlockIds.isEmpty()) {
//                int[] blockEntityIds1 = {firstId};
//                int[] blockEntityIds2 = Converter.convertListToArray(newBlockIds);
//                blocks.add(new BilateralBlock(blockEntityIds1, blockEntityIds2));
//            } 
//        }
//        return blocks;
//    }
//
//    @Override
//    protected List<AbstractBlock> getUnilateralBlocks() {
//        int noOfEntities = profiles1.length;
//        final Set<Integer> entityIds = new HashSet<Integer>(2 * noOfEntities);
//        for (int i = 0; i < noOfEntities; i++) {
//            entityIds.add(i);
//        }
//
//        final List<AbstractBlock> blocks = new ArrayList<AbstractBlock>();
//        while (!entityIds.isEmpty()) {
//            // Get first element:
//            Iterator iter = entityIds.iterator();
//            int firstId = (Integer) iter.next();
//
//            // Remove first element:
//            iter.remove();
//            entityIds.remove(firstId);
//
//            // Start a new cluster:
//            final List<Integer> newBlockIds = new ArrayList<Integer>();
//            newBlockIds.add(firstId);
//
//            minimumWeight = Double.MIN_VALUE;
//            n2NearestEntities = new PriorityQueue<Comparison>((int) (2 * n2), new ComparisonWeightComparator());
//
//            // Compare to remaining objects:
//            while (iter.hasNext()) {
//                int currentId = (Integer) iter.next();
//                double jaccardSim = getJaccardSimilarity(profiles1[firstId], profiles1[currentId]);
//
//                Comparison comparison = new Comparison(false, firstId, currentId);
//                comparison.setUtilityMeasure(jaccardSim);
//                addComparison(comparison);
//            }
//
//            for (int i = 0; i < n2 && !n2NearestEntities.isEmpty(); i++) {
//                Comparison lastComparison = (Comparison) n2NearestEntities.poll();
//                newBlockIds.add(lastComparison.getEntityId2());
//                if (i >= (n2 - n1)) {
//                    entityIds.remove(lastComparison.getEntityId2());
//                }
//            }
//
//            if (1 < newBlockIds.size()) {
//                int[] blockEntityIds = Converter.convertListToArray(newBlockIds);
//                blocks.add(new UnilateralBlock(blockEntityIds));
//            }
//        }
//        return blocks;
//    }
//
//	@Override
//	public void buildQueryBlocks() {
//		
//		
//	}
//}