
package org.imsi.queryEREngine.imsi.er.BlockBuilding;

import org.imsi.queryEREngine.imsi.er.DataStructures.AbstractBlock;
import org.imsi.queryEREngine.imsi.er.DataStructures.BilateralBlock;
import org.imsi.queryEREngine.imsi.er.DataStructures.EntityIndex;
import org.imsi.queryEREngine.imsi.er.DataStructures.EntityProfile;
import org.imsi.queryEREngine.imsi.er.DataStructures.UnilateralBlock;
import org.imsi.queryEREngine.imsi.er.Utilities.Converter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class AbstractCanopies extends AbstractQGramsBlocking {

    protected int noOfBlocks;
    protected int datasetLimit;
    protected int totalEntities;
    protected int[] flags;
    protected double[] counters;

    protected List<AbstractBlock> qGramBlocks;
   

	protected BilateralBlock[] bBlocks;
    protected EntityIndex entityIndex;
    protected final List<Integer> neighbors;
    protected final List<Integer> retainedNeighbors;
    protected final Set<Integer> removedEntities;
    protected final Set<Integer> validEntities;
    protected UnilateralBlock[] uBlocks;

    public AbstractCanopies(int n, String description, List<EntityProfile>[] profiles) {
        super(n, description, profiles);
        neighbors = new ArrayList<>();
        retainedNeighbors = new ArrayList<>();
        removedEntities = new HashSet<>();
        validEntities = new HashSet<>();
    }

    public AbstractCanopies(int n, String description, String[] entities, String[] index) {
        super(n, description, entities, index);
        neighbors = new ArrayList<>();
        retainedNeighbors = new ArrayList<>();
        removedEntities = new HashSet<>();
        validEntities = new HashSet<>();
    }

    protected abstract void getBilateralBlocks();

    protected abstract void getUnilateralBlocks();

    protected void addBilateralBlock(int entityId) {
        if (!retainedNeighbors.isEmpty()) {
            int[] blockEntityIds1 = {entityId};
            int[] blockEntityIds2 = Converter.convertCollectionToArray(retainedNeighbors);
            blocks.add(new BilateralBlock(blockEntityIds1, blockEntityIds2));
        }
    }

    protected void addUnilateralBlock(int entityId) {
        if (!retainedNeighbors.isEmpty()) {
            retainedNeighbors.add(entityId);
            int[] blockEntityIds = Converter.convertCollectionToArray(retainedNeighbors);
            blocks.add(new UnilateralBlock(blockEntityIds));
        }
    }
    

    @Override
    public List<AbstractBlock> buildBlocks() {
    	
        entityIndex = new EntityIndex(qGramBlocks);

        noOfBlocks = qGramBlocks.size();
        datasetLimit = entityIndex.getDatasetLimit();
        totalEntities = entityIndex.getNoOfEntities();
        counters = new double[totalEntities];
        flags = new int[totalEntities];
        for (int i = 0; i < totalEntities; i++) {
            flags[i] = -1;
        }

        int counter = 0;
        if (cleanCleanER) {
            bBlocks = new BilateralBlock[noOfBlocks];
            for (AbstractBlock block : qGramBlocks) {
                bBlocks[counter++] = (BilateralBlock) block;
            }
            qGramBlocks.clear();

            getBilateralBlocks();
        } else {
            uBlocks = new UnilateralBlock[noOfBlocks];
            for (AbstractBlock block : qGramBlocks) {
                uBlocks[counter++] = (UnilateralBlock) block;
            }
            qGramBlocks.clear();

            getUnilateralBlocks();
        }
        return blocks;
    }

    protected void setNeighborEntities(int blockIndex, int entityId) {
        neighbors.clear();
        if (cleanCleanER) {
            if (entityId < datasetLimit) {
                for (int originalId : bBlocks[blockIndex].getIndex2Entities()) {
                    neighbors.add(originalId);
                }
            } else {
                for (int originalId : bBlocks[blockIndex].getIndex1Entities()) {
                    neighbors.add(originalId);
                }
            }
        } else {
            for (int neighborId : uBlocks[blockIndex].getEntities()) {
                if (neighborId != entityId) {
                    neighbors.add(neighborId);
                }
            }
        }
    }

    protected void setUnilateralValidEntities(int entityId) {
        validEntities.clear();
        final int[] associatedBlocks = entityIndex.getEntityBlocks(entityId, 0);
        if (associatedBlocks.length == 0) {
            return;
        }

        for (int blockIndex : associatedBlocks) {
            setNeighborEntities(blockIndex, entityId);
            for (int neighborId : neighbors) {
                if (!removedEntities.contains(neighborId)) {
                    if (flags[neighborId] != entityId) {
                        counters[neighborId] = 0;
                        flags[neighborId] = entityId;
                    }

                    counters[neighborId]++;
                    validEntities.add(neighborId);
                }
            }
        }
    }

    protected void setBilateralValidEntities(int entityId) {
        validEntities.clear();
        final int[] associatedBlocks = entityIndex.getEntityBlocks(entityId, 0);
        if (associatedBlocks.length == 0) {
            return;
        }

        for (int blockIndex : associatedBlocks) {
            setNeighborEntities(blockIndex, entityId);
            for (int neighborId : neighbors) {
                if (!removedEntities.contains(neighborId)) {
                    if (flags[neighborId] != entityId) {
                        counters[neighborId] = 0;
                        flags[neighborId] = entityId;
                    }

                    counters[neighborId]++;
                    validEntities.add(neighborId);
                }
            }
        }
    }
    
    public List<AbstractBlock> getqGramBlocks() {
		return qGramBlocks;
	}

	public void setqGramBlocks(List<AbstractBlock> qGramBlocks) {
		this.qGramBlocks = qGramBlocks;
	}
}
