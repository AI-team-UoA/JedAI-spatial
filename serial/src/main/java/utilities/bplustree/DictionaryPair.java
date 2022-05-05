package utilities.bplustree;

import org.locationtech.jts.geom.Envelope;

public class DictionaryPair implements Comparable<DictionaryPair> {
    int key;
    double value;
    Envelope internal;
    int index;

    public DictionaryPair(int key, double value, int index, Envelope internal) {
        this.key = key;
        this.value = value;
        this.index = index;
        this.internal = internal;
    }

    @Override
    public int compareTo(DictionaryPair o) {
        if (key == o.key) {
            return 0;
        } else if (key > o.key) {
            return 1;
        } else {
            return -1;
        }
    }
}
