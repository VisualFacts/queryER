

package org.imsi.queryEREngine.imsi.er.BlockBuilding;
import org.imsi.queryEREngine.imsi.er.DataStructures.EntityProfile;
import java.util.HashSet;
import java.util.List;
import java.util.Set;



public abstract class AbstractQGramsBlocking extends AbstractTokenBlocking {

    protected final int nGramSize;

    public AbstractQGramsBlocking(int n, List<EntityProfile>[] profiles) {
        this(n, "Memory-based Character N-Grams Blocking", profiles);
    }

    public AbstractQGramsBlocking(int n, String description, List<EntityProfile>[] profiles) {
        super(description, profiles);
        nGramSize = n;
    }

    public AbstractQGramsBlocking(int n, String[] entities, String[] index) {
        this(n, "Disk-based Character N-Grams Blocking", entities, index);
    }

    public AbstractQGramsBlocking(int n, String description, String[] entities, String[] index) {
        super(description, entities, index);
        nGramSize = n;
    }

    @Override
    protected Set<String> getBlockingKeys(String attributeValue) {
        final Set<String> nGrams = new HashSet<>();
        for (String token : getTokens(attributeValue)) {
            nGrams.addAll(Utilities.getNGrams(nGramSize, token));
        }

        return nGrams;
    }
}
