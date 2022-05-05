package utilities.bplustree;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import org.locationtech.jts.geom.Envelope;

import java.util.*;

public class BPlusTree {
    int m; // aka branching factor
    InternalNode root;
    LeafNode firstLeaf;

    // Binary search program
    private int binarySearch(DictionaryPair[] dps, int numPairs, int t) {
        Comparator<DictionaryPair> c = new Comparator<DictionaryPair>() {
            @Override
            public int compare(DictionaryPair o1, DictionaryPair o2) {
                Integer a = Integer.valueOf(o1.key);
                Integer b = Integer.valueOf(o2.key);
                return a.compareTo(b);
            }
        };
        return Arrays.binarySearch(dps, 0, numPairs, new DictionaryPair(t, 0, 0, null), c);
    }

    // Find the leaf node
    private LeafNode findLeafNode(int key) {

        Integer[] keys = this.root.keys;
        int i;

        for (i = 0; i < this.root.degree - 1; i++) {
            if (key < keys[i]) {
                break;
            }
        }

        Node child = this.root.childPointers[i];
        if (child instanceof LeafNode) {
            return (LeafNode) child;
        } else {
            return findLeafNode((InternalNode) child, key);
        }
    }

    // Find the leaf node
    private LeafNode findLeafNode(InternalNode node, int key) {

        Integer[] keys = node.keys;
        int i;

        for (i = 0; i < node.degree - 1; i++) {
            if (key < keys[i]) {
                break;
            }
        }
        Node childNode = node.childPointers[i];
        if (childNode instanceof LeafNode) {
            return (LeafNode) childNode;
        } else {
            return findLeafNode((InternalNode) node.childPointers[i], key);
        }
    }

    // Finding the index of the pointer
    private int findIndexOfPointer(Node[] pointers, LeafNode node) {
        int i;
        for (i = 0; i < pointers.length; i++) {
            if (pointers[i] == node) {
                break;
            }
        }
        return i;
    }

    // Get the mid point
    private int getMidpoint() {
        return (int) Math.ceil((this.m + 1) / 2.0) - 1;
    }

    // Balance the tree
    private void handleDeficiency(InternalNode in) {

        InternalNode sibling;
        InternalNode parent = in.parent;

        if (this.root == in) {
            for (int i = 0; i < in.childPointers.length; i++) {
                if (in.childPointers[i] != null) {
                    if (in.childPointers[i] instanceof InternalNode) {
                        this.root = (InternalNode) in.childPointers[i];
                        this.root.parent = null;
                    } else if (in.childPointers[i] instanceof LeafNode) {
                        this.root = null;
                    }
                }
            }
        }

        else if (in.leftSibling != null && in.leftSibling.isLendable()) {
            sibling = in.leftSibling;
        } else if (in.rightSibling != null && in.rightSibling.isLendable()) {
            sibling = in.rightSibling;

            int borrowedKey = sibling.keys[0];
            Node pointer = sibling.childPointers[0];

            in.keys[in.degree - 1] = parent.keys[0];
            in.childPointers[in.degree] = pointer;

            parent.keys[0] = borrowedKey;

            sibling.removePointer(0);
            Arrays.sort(sibling.keys);
            sibling.removePointer(0);
            shiftDown(in.childPointers, 1);
        } else if (in.leftSibling != null && in.leftSibling.isMergeable()) {

        } else if (in.rightSibling != null && in.rightSibling.isMergeable()) {
            sibling = in.rightSibling;
            sibling.keys[sibling.degree - 1] = parent.keys[parent.degree - 2];
            Arrays.sort(sibling.keys, 0, sibling.degree);
            parent.keys[parent.degree - 2] = null;

            for (int i = 0; i < in.childPointers.length; i++) {
                if (in.childPointers[i] != null) {
                    sibling.prependChildPointer(in.childPointers[i]);
                    in.childPointers[i].parent = sibling;
                    in.removePointer(i);
                }
            }

            parent.removePointer(in);

            sibling.leftSibling = in.leftSibling;
        }

        if (parent != null && parent.isDeficient()) {
            handleDeficiency(parent);
        }
    }

    private boolean isEmpty() {
        return firstLeaf == null;
    }

    protected int linearNullSearch(DictionaryPair[] dps) {
        for (int i = 0; i < dps.length; i++) {
            if (dps[i] == null) {
                return i;
            }
        }
        return -1;
    }

    protected int linearNullSearch(Node[] pointers) {
        for (int i = 0; i < pointers.length; i++) {
            if (pointers[i] == null) {
                return i;
            }
        }
        return -1;
    }

    private void shiftDown(Node[] pointers, int amount) {
        Node[] newPointers = new Node[this.m + 1];
        for (int i = amount; i < pointers.length; i++) {
            newPointers[i - amount] = pointers[i];
        }
        pointers = newPointers;
    }

    private void sortDictionary(DictionaryPair[] dictionary) {
        Arrays.sort(dictionary, new Comparator<DictionaryPair>() {
            @Override
            public int compare(DictionaryPair o1, DictionaryPair o2) {
                if (o1 == null && o2 == null) {
                    return 0;
                }
                if (o1 == null) {
                    return 1;
                }
                if (o2 == null) {
                    return -1;
                }
                return o1.compareTo(o2);
            }
        });
    }

    private Node[] splitChildPointers(InternalNode in, int split) {

        Node[] pointers = in.childPointers;
        Node[] halfPointers = new Node[this.m + 1];

        for (int i = split + 1; i < pointers.length; i++) {
            halfPointers[i - split - 1] = pointers[i];
            in.removePointer(i);
        }

        return halfPointers;
    }

    private DictionaryPair[] splitDictionary(LeafNode ln, int split) {

        DictionaryPair[] dictionary = ln.dictionary;

        DictionaryPair[] halfDict = new DictionaryPair[this.m];

        for (int i = split; i < dictionary.length; i++) {
            halfDict[i - split] = dictionary[i];
            ln.delete(i);
        }

        return halfDict;
    }

    private void splitInternalNode(InternalNode in) {

        InternalNode parent = in.parent;

        int midpoint = getMidpoint();
        int newParentKey = in.keys[midpoint];
        Integer[] halfKeys = splitKeys(in.keys, midpoint);
        Node[] halfPointers = splitChildPointers(in, midpoint);

        in.degree = linearNullSearch(in.childPointers);

        InternalNode sibling = new InternalNode(this, this.m, halfKeys, halfPointers);
        for (Node pointer : halfPointers) {
            if (pointer != null) {
                pointer.parent = sibling;
            }
        }

        sibling.rightSibling = in.rightSibling;
        if (sibling.rightSibling != null) {
            sibling.rightSibling.leftSibling = sibling;
        }
        in.rightSibling = sibling;
        sibling.leftSibling = in;

        if (parent == null) {

            Integer[] keys = new Integer[this.m];
            keys[0] = newParentKey;
            InternalNode newRoot = new InternalNode(this, this.m, keys);
            newRoot.appendChildPointer(in);
            newRoot.appendChildPointer(sibling);
            this.root = newRoot;

            in.parent = newRoot;
            sibling.parent = newRoot;

        } else {

            parent.keys[parent.degree - 1] = newParentKey;
            Arrays.sort(parent.keys, 0, parent.degree);

            int pointerIndex = parent.findIndexOfPointer(in) + 1;
            parent.insertChildPointer(sibling, pointerIndex);
            sibling.parent = parent;
        }
    }

    private Integer[] splitKeys(Integer[] keys, int split) {

        Integer[] halfKeys = new Integer[this.m];

        keys[split] = null;

        for (int i = split + 1; i < keys.length; i++) {
            halfKeys[i - split - 1] = keys[i];
            keys[i] = null;
        }

        return halfKeys;
    }

    public void insert(int key, double value, int index, Envelope internal) {
        if (isEmpty()) {

            LeafNode ln = new LeafNode(this, this.m, new DictionaryPair(key, value, index, internal));

            this.firstLeaf = ln;

        } else {
            LeafNode ln = (this.root == null) ? this.firstLeaf : findLeafNode(key);

            if (!ln.insert(new DictionaryPair(key, value, index, internal))) {

                ln.dictionary[ln.numPairs] = new DictionaryPair(key, value, index, internal);
                ln.numPairs++;
                sortDictionary(ln.dictionary);

                int midpoint = getMidpoint();
                DictionaryPair[] halfDict = splitDictionary(ln, midpoint);

                if (ln.parent == null) {

                    Integer[] parent_keys = new Integer[this.m];
                    parent_keys[0] = halfDict[0].key;
                    InternalNode parent = new InternalNode(this, this.m, parent_keys);
                    ln.parent = parent;
                    parent.appendChildPointer(ln);

                } else {
                    int newParentKey = halfDict[0].key;
                    ln.parent.keys[ln.parent.degree - 1] = newParentKey;
                    Arrays.sort(ln.parent.keys, 0, ln.parent.degree);
                }

                LeafNode newLeafNode = new LeafNode(this, this.m, halfDict, ln.parent);

                int pointerIndex = ln.parent.findIndexOfPointer(ln) + 1;
                ln.parent.insertChildPointer(newLeafNode, pointerIndex);

                newLeafNode.rightSibling = ln.rightSibling;
                if (newLeafNode.rightSibling != null) {
                    newLeafNode.rightSibling.leftSibling = newLeafNode;
                }
                ln.rightSibling = newLeafNode;
                newLeafNode.leftSibling = ln;

                if (this.root == null) {

                    this.root = ln.parent;

                } else {
                    InternalNode in = ln.parent;
                    while (in != null) {
                        if (in.isOverfull()) {
                            splitInternalNode(in);
                        } else {
                            break;
                        }
                        in = in.parent;
                    }
                }
            }
        }
    }

    public Double search(int key) {

        if (isEmpty()) {
            return null;
        }

        LeafNode ln = (this.root == null) ? this.firstLeaf : findLeafNode(key);

        DictionaryPair[] dps = ln.dictionary;
        int index = binarySearch(dps, ln.numPairs, key);

        if (index < 0) {
            return null;
        } else {
            return dps[index].value;
        }
    }

//    public TIntList search(double lowerBound, double upperBound) {
//
//        TIntList geometryIndexes = new TIntArrayList();
//
//        LeafNode currNode = this.firstLeaf;
//        while (currNode != null) {
//
//            DictionaryPair dps[] = currNode.dictionary;
//            for (DictionaryPair dp : dps) {
//
//                if (dp == null) {
//                    break;
//                }
//
//                if ((lowerBound <= dp.key + dp.value && dp.key + dp.value <= upperBound)
//                || (lowerBound <= dp.upper && dp.upper <= upperBound)) {
//                    geometryIndexes.add(dp.index);
//                }
//
//
//            }
//            currNode = currNode.rightSibling;
//
//        }
//
//        return geometryIndexes;
//    }

    public TIntList search(Envelope envelope) {

        TIntList geometryIndexes = new TIntArrayList();

        LeafNode currNode = this.firstLeaf;
        while (currNode != null) {

            DictionaryPair dps[] = currNode.dictionary;
            for (DictionaryPair dp : dps) {

                if (dp == null)
                    break;

                if (envelope.intersects(dp.internal))
                    geometryIndexes.add(dp.index);

            }
            currNode = currNode.rightSibling;
        }

        return geometryIndexes;
    }

    public BPlusTree(int m) {
        this.m = m;
        this.root = null;
    }

}