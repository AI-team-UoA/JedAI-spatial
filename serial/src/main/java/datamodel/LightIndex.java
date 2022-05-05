package datamodel;

import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;

public class LightIndex {

    private final TIntObjectMap<TIntObjectMap<TIntList>> map;

    public LightIndex() {
        map = new TIntObjectHashMap<>();
    }

    public void add(int lat, int lon, int sourceId) {
        TIntObjectMap<TIntList> list = map.get(lat);
        if (list == null) {
            list = new TIntObjectHashMap<>();
            map.put(lat, list);
        }

        TIntList index = list.get(lon);
        if (index == null) {
            index = new TIntArrayList();
            list.put(lon, index);
        }
        
        index.add(sourceId);
    }

    public TIntList getSquare(int lat, int lon) {
        final TIntObjectMap<TIntList> list = map.get(lat);
        if (list == null) {
            return null;
        }

        return list.get(lon);
    }
    
    public TIntObjectMap<TIntObjectMap<TIntList>> getMap() {
        return map;
    }
}
