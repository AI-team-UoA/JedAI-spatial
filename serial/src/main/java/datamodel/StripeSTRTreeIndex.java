package datamodel;

import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import lombok.Getter;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.index.strtree.STRtree;
import utilities.bplustree.BPlusTree;

/**
 * Similar to LightIndex: instead of having nxm tiles it has n tiles Indexing is
 * only based on the x coordinate
 */
@Getter
public class StripeSTRTreeIndex {

    private final int m;
    private final TIntObjectMap<STRtree> map;

    public StripeSTRTreeIndex() {
        this.map = new TIntObjectHashMap<>();
        this.m = 10;
    }

    public void add(int lon, Envelope internal, int geometryIndex) {
        STRtree index = map.get(lon);
        if (index == null) {
            index = new STRtree(this.m);
            map.put(lon, index);
        }

        index.insert(internal, geometryIndex);
    }

    public STRtree getSTRTree(int lon) {
        return map.get(lon);
    }
}
