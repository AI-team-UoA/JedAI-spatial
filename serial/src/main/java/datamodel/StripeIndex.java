package datamodel;

import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import lombok.Getter;

/**
 * Similar to LightIndex: instead of having nxm tiles it has n tiles
 * Indexing is only based on the x coordinate
 */
@Getter
public class StripeIndex {

    private final TIntObjectMap<TIntList> map;

    public StripeIndex() {
        this.map = new TIntObjectHashMap<>();
    }

    public void add(int lon, int sourceId) {

        TIntList index = map.get(lon);
        if (index == null) {
            index = new TIntArrayList();
            map.put(lon, index);
        }

        index.add(sourceId);
    }

    public TIntList getStripe(int lon) {
        return map.get(lon);
    }

}
