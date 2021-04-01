

package org.imsi.queryEREngine.imsi.er.BlockBuilding;

import org.imsi.queryEREngine.imsi.er.DataStructures.AbstractBlock;
import java.util.List;



public abstract class AbstractBlockingMethod {

    private final String name;

    public AbstractBlockingMethod(String nm) {
        name = nm;
    }

    public String getName() {
        return name;
    }

    public abstract List<AbstractBlock> buildBlocks();
}
