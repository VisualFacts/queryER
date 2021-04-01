package org.imsi.queryEREngine.imsi.er.EfficiencyLayer.MemoryBased;

import java.util.List;
import org.imsi.queryEREngine.imsi.er.DataStructures.AbstractBlock;


public abstract class AbstractBlockingMethod {
    
    private final String name;
    
    public AbstractBlockingMethod(String nm) {
        name = nm;
    }

    public String getName() {
        return name;
    }
    
    public abstract List<AbstractBlock> buildBlocks();
    public abstract void buildQueryBlocks();
    
}