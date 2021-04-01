

package org.imsi.queryEREngine.imsi.er.BlockBuilding;

import org.imsi.queryEREngine.imsi.er.DataStructures.EntityProfile;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public abstract class AbstractTokenBlocking extends AbstractIndexBasedMethod {

    public AbstractTokenBlocking(List<EntityProfile>[] profiles) {
        super("Memory-based Token Blocking", profiles);
    }

    public AbstractTokenBlocking(String description, List<EntityProfile>[] profiles) {
        super(description, profiles);
    }

    public AbstractTokenBlocking(String[] entities, String[] index) {
        this("Disk-based Token Blocking", entities, index);
    }

    public AbstractTokenBlocking(String description, String[] entities, String[] index) {
        super(description, entities, index);
    }

    @Override
    protected Set<String> getBlockingKeys(String attributeValue) {
        return new HashSet<>(Arrays.asList(getTokens(attributeValue)));
    }

    protected String[] getTokens (String attributeValue) {
        return attributeValue.split("[\\W_]");
    }
}
