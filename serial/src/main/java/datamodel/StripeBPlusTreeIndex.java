package datamodel;

import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import lombok.Getter;
import org.locationtech.jts.geom.Envelope;
import utilities.bplustree.BPlusTree;

/**
 * Similar to LightIndex: instead of having nxm tiles it has n tiles Indexing is
 * only based on the x coordinate
 */
@Getter
public class StripeBPlusTreeIndex {

    private final int m;
    private final TIntObjectMap<BPlusTree> map;

    public StripeBPlusTreeIndex() {
        this.map = new TIntObjectHashMap<>();
        this.m = 5;
    }

    public void add(int lon, int key, double value, Envelope internal, int geometryIndex) {
        BPlusTree index = map.get(lon);
        if (index == null) {
            index = new BPlusTree(this.m);
            map.put(lon, index);
        }

        index.insert(key, value, geometryIndex, internal);
    }

    public BPlusTree getBPlusTree(int lon) {
        return map.get(lon);
    }

}
