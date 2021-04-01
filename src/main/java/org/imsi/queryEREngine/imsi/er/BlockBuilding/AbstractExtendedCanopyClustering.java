

package org.imsi.queryEREngine.imsi.er.BlockBuilding;

import org.imsi.queryEREngine.imsi.er.Comparators.ComparisonWeightComparator;
import org.imsi.queryEREngine.imsi.er.DataStructures.Comparison;
import org.imsi.queryEREngine.imsi.er.DataStructures.EntityProfile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

public abstract class AbstractExtendedCanopyClustering extends AbstractCanopies {

    private final int n1;
    private final int n2;
    private double minimumWeight;
    private final Queue<Comparison> n2NearestEntities;

    public AbstractExtendedCanopyClustering(int n1, int n2, int n, List<EntityProfile>[] profiles) {
        super(n, "In-memory Canopy Clustering with Nearest Neighbors", profiles);
        this.n1 = n1;
        this.n2 = n2;
        n2NearestEntities = new PriorityQueue<>(2 * n2, new ComparisonWeightComparator());
    }

    public AbstractExtendedCanopyClustering(int n1, int n2, int n, String[] entityPaths, String[] index) {
        super(n, "In-memory Canopy Clustering with Nearest Neighbors", entityPaths, index);
        this.n1 = n1;
        this.n2 = n2;
        n2NearestEntities = new PriorityQueue<>(2 * n2, new ComparisonWeightComparator());
    }

    private void addComparison(Comparison comparison) {
        if (comparison.getUtilityMeasure() < minimumWeight) {
            return;
        }

        n2NearestEntities.add(comparison);
        if (n2 < n2NearestEntities.size()) {
            Comparison lastComparison = n2NearestEntities.poll();
            minimumWeight = lastComparison.getUtilityMeasure();
        }
    }

    @Override
    protected void getBilateralBlocks() {
        final List<Integer> entityIds1 = new ArrayList<>();
        for (int i = 0; i < datasetLimit; i++) {
            entityIds1.add(i);
        }
        Collections.shuffle(entityIds1);

        removedEntities.clear();
        int d2Entities = totalEntities - datasetLimit;
        final Iterator iterator = entityIds1.iterator();
        while (iterator.hasNext() && removedEntities.size() < d2Entities) {
            // Get current element:
            int currentId = (Integer) iterator.next();

            // Start a new cluster:
            minimumWeight = Double.MIN_VALUE;
            n2NearestEntities.clear();
            setBilateralValidEntities(currentId);

            // Compare to remaining objects:
            for (int neighborId : validEntities) {
                double jaccardSim = counters[neighborId] / (entityIndex.getNoOfEntityBlocks(currentId, 0) + entityIndex.getNoOfEntityBlocks(neighborId, 1) - counters[neighborId]);

                Comparison comparison = new Comparison(false, currentId, neighborId);
                comparison.setUtilityMeasure(jaccardSim);
                addComparison(comparison);
            }

            retainedNeighbors.clear();
            int noOfBestNeighbors = Math.min(n2, n2NearestEntities.size());
            for (int i = 0; i < noOfBestNeighbors; i++) {
                retainedNeighbors.add(null);
            }

            for (int i = 0; i < noOfBestNeighbors; i++) {
                Comparison lastComparison = n2NearestEntities.poll();
                retainedNeighbors.set(noOfBestNeighbors - i - 1, lastComparison.getEntityId2());
            }

            int mostSimilar = Math.min(n1, noOfBestNeighbors);
            for (int i = 0; i < mostSimilar; i++) {
                removedEntities.add(retainedNeighbors.get(i));
            }

            addBilateralBlock(currentId);
        }
    }

    @Override
    protected void getUnilateralBlocks() {
       final List<Integer> entityIds = new ArrayList<>();
        for (int i = 0; i < totalEntities; i++) {
            entityIds.add(i);
        }
        Collections.shuffle(entityIds);

        removedEntities.clear();
        final Iterator iter = entityIds.iterator();
        while (removedEntities.size() < totalEntities) {
            // Get next element:
            int currentId = (Integer) iter.next();

            // Remove first element:
            removedEntities.add(currentId);

            // Start a new cluster:
            minimumWeight = Double.MIN_VALUE;
            n2NearestEntities.clear();
            setUnilateralValidEntities(currentId);

            // Compare to remaining objects:
            for (int neighborId : validEntities) {
                double jaccardSim = counters[neighborId] / (entityIndex.getNoOfEntityBlocks(currentId, 0) + entityIndex.getNoOfEntityBlocks(neighborId, 0) - counters[neighborId]);

                Comparison comparison = new Comparison(false, currentId, neighborId);
                comparison.setUtilityMeasure(jaccardSim);
                addComparison(comparison);
            }

            retainedNeighbors.clear();
            int noOfBestNeighbors = Math.min(n2, n2NearestEntities.size());
            for (int i = 0; i < noOfBestNeighbors; i++) {
                retainedNeighbors.add(null);
            }

            for (int i = 0; i < noOfBestNeighbors; i++) {
                Comparison lastComparison = n2NearestEntities.poll();
                retainedNeighbors.set(noOfBestNeighbors - i - 1, lastComparison.getEntityId2());
            }

            int mostSimilar = Math.min(n1, noOfBestNeighbors);
            for (int i = 0; i < mostSimilar; i++) {
                removedEntities.add(retainedNeighbors.get(i));
            }

            addUnilateralBlock(currentId);
        }
    }
}
