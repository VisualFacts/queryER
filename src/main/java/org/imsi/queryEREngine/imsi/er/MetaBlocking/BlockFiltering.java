package  org.imsi.queryEREngine.imsi.er.MetaBlocking;

import java.util.*;
import java.util.stream.Collectors;

import org.imsi.queryEREngine.imsi.er.Comparators.BlockCardinalityComparator;
import org.imsi.queryEREngine.imsi.er.DataStructures.AbstractBlock;
import org.imsi.queryEREngine.imsi.er.DataStructures.BilateralBlock;
import org.imsi.queryEREngine.imsi.er.DataStructures.UnilateralBlock;
import org.imsi.queryEREngine.imsi.er.EfficiencyLayer.AbstractEfficiencyMethod;
import org.imsi.queryEREngine.imsi.er.Utilities.Converter;

public class BlockFiltering extends AbstractEfficiencyMethod {

    protected final double ratio;

    protected int entitiesD1;
    protected int entitiesD2;
    protected int[] counterD1;
    protected int[] counterD2;
    protected int[] limitsD1;
    protected int[] limitsD2;

    public BlockFiltering(double r) {
        this(r, "Block Filtering");
    }

    public BlockFiltering(double r, String description) {
        super(description);
        ratio = r;
    }

    @Override
    public void applyProcessing(List<AbstractBlock> blocks) {
        countEntities(blocks);
        sortBlocks(blocks);
        getLimits(blocks);
        initializeCounters();
        restructureBlocks(blocks);
    }

    protected void countEntities(List<AbstractBlock> blocks) {
        entitiesD1 = Integer.MIN_VALUE;
        entitiesD2 = Integer.MIN_VALUE;
        if (blocks.get(0) instanceof BilateralBlock) {
            for (AbstractBlock block : blocks) {
                BilateralBlock bilBlock = (BilateralBlock) block;
                for (int id1 : bilBlock.getIndex1Entities()) {
                    if (entitiesD1 < id1 + 1) {
                        entitiesD1 = id1 + 1;
                    }
                }
                for (int id2 : bilBlock.getIndex2Entities()) {
                    if (entitiesD2 < id2 + 1) {
                        entitiesD2 = id2 + 1;
                    }
                }
            }
        } else if (blocks.get(0) instanceof UnilateralBlock) {
            for (AbstractBlock block : blocks) {
                UnilateralBlock uniBlock = (UnilateralBlock) block;
                for (int id : uniBlock.getEntities()) {
                    if (entitiesD1 < id + 1) {
                        entitiesD1 = id + 1;
                    }
                }
            }
        }
    }

    protected void getBilateralLimits(List<AbstractBlock> blocks) {
        limitsD1 = new int[entitiesD1];
        limitsD2 = new int[entitiesD2];
        for (AbstractBlock block : blocks) {
            BilateralBlock bilBlock = (BilateralBlock) block;
            for (int id1 : bilBlock.getIndex1Entities()) {
                limitsD1[id1]++;
            }
            for (int id2 : bilBlock.getIndex2Entities()) {
                limitsD2[id2]++;
            }
        }

        for (int i = 0; i < limitsD1.length; i++) {
            limitsD1[i] = (int) Math.round(ratio * limitsD1[i]);
        }
        for (int i = 0; i < limitsD2.length; i++) {
            limitsD2[i] = (int) Math.round(ratio * limitsD2[i]);
        }
    }

    protected void getLimits(List<AbstractBlock> blocks) {
        if (blocks.get(0) instanceof BilateralBlock) {
            getBilateralLimits(blocks);
        } else if (blocks.get(0) instanceof UnilateralBlock) {
            getUnilateralLimits(blocks);
        }
    }

    protected void getUnilateralLimits(List<AbstractBlock> blocks) {
        limitsD1 = new int[entitiesD1];
        limitsD2 = null;
        for (AbstractBlock block : blocks) {
            UnilateralBlock uniBlock = (UnilateralBlock) block;
            for (int id : uniBlock.getEntities()) {
                limitsD1[id]++;
            }
        }

        for (int i = 0; i < limitsD1.length; i++) {
            limitsD1[i] = (int) Math.round(ratio * limitsD1[i]);
        }
    }

    protected void initializeCounters() {
        counterD1 = new int[entitiesD1];
        counterD2 = null;
        if (0 < entitiesD2) {
            counterD2 = new int[entitiesD2];
        }
    }

    protected void restructureBilateraBlocks(List<AbstractBlock> blocks) {
        final List<AbstractBlock> newBlocks = new ArrayList<AbstractBlock>();
        for (AbstractBlock block : blocks) {
            BilateralBlock oldBlock = (BilateralBlock) block;
            final List<Integer> retainedEntitiesD1 = new ArrayList<Integer>();
            for (int entityId : oldBlock.getIndex1Entities()) {
                if (counterD1[entityId] < limitsD1[entityId]) {
                    retainedEntitiesD1.add(entityId);
                }
            }

            final List<Integer> retainedEntitiesD2 = new ArrayList<Integer>();
            for (int entityId : oldBlock.getIndex2Entities()) {
                if (counterD2[entityId] < limitsD2[entityId]) {
                    retainedEntitiesD2.add(entityId);
                }
            }

            if (!retainedEntitiesD1.isEmpty() && !retainedEntitiesD2.isEmpty()) {
                int[] blockEntitiesD1 = Converter.convertListToArray(retainedEntitiesD1);
                for (int entityId : blockEntitiesD1) {
                    counterD1[entityId]++;
                }
                int[] blockEntitiesD2 = Converter.convertListToArray(retainedEntitiesD2);
                for (int entityId : blockEntitiesD2) {
                    counterD2[entityId]++;
                }
                newBlocks.add(new BilateralBlock(blockEntitiesD1, blockEntitiesD2));
            }
        }
        blocks.clear();
        blocks.addAll(newBlocks);
    }

    protected void restructureBlocks(List<AbstractBlock> blocks) {
        if (blocks.get(0) instanceof BilateralBlock) {
            restructureBilateraBlocks(blocks);
        } else if (blocks.get(0) instanceof UnilateralBlock) {
            restructureUnilateraBlocks(blocks);
        }
    }

    protected void restructureUnilateraBlocks(List<AbstractBlock> blocks) {
        final List<AbstractBlock> newBlocks = new ArrayList<AbstractBlock>();

        for (AbstractBlock block : blocks) {
            UnilateralBlock oldBlock = (UnilateralBlock) block;
            int[] queryEntities = oldBlock.getQueryEntities();
            final Set<Integer> retainedEntities = new HashSet<>();
            for (int entityId : oldBlock.getEntities()) {
                if (counterD1[entityId] < limitsD1[entityId]) {
                    retainedEntities.add(entityId);
                }
                //retainedEntities.addAll(Arrays.stream(queryEntities).boxed().collect(Collectors.toList()));
            }

            if (1 < retainedEntities.size()) {
                int[] blockEntities = Converter.convertListToArray(retainedEntities);
                for (int entityId : blockEntities) {
                    counterD1[entityId]++;
                }
                UnilateralBlock nb = new UnilateralBlock(blockEntities);
                nb.setQueryEntities(Arrays.stream(queryEntities).filter(retainedEntities::contains).toArray());
                if(nb.getQueryEntities().length==0) continue;
//                nb.setQueryEntities(queryEntities);
                newBlocks.add(nb);
            }
        }
        blocks.clear();
        blocks.addAll(newBlocks);
    }

    protected void sortBlocks(List<AbstractBlock> blocks) {
        Collections.sort(blocks, new BlockCardinalityComparator());
    }
}
