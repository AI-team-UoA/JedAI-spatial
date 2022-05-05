package datamodel.rtree;

import datamodel.Mbr;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@Getter
@Setter
public class Node {
    private int degree;
    private int maxDegree;

    protected Mbr mbr;
    protected Node[] childPointers;
    protected InternalNode parent;
    protected Mbr[] keys;

    public Node(int maxDegree) {
        this.mbr = new Mbr();
        this.parent = null;

        this.degree = 0;
        this.maxDegree = maxDegree;

        // maxDegree + 1 because when splitting M + 1 keys are appended to these arrays
        this.keys = new Mbr[maxDegree + 1];
        this.childPointers = new Node[maxDegree + 1];
    }

    public Node(Node node) {
        this.degree = node.degree;
        this.maxDegree = node.maxDegree;
        this.mbr = node.mbr;
        this.parent = node.parent;

        this.keys = new Mbr[maxDegree + 1];
        for (short i=0; i<node.keys.length; i++) {
            this.keys[i] = node.keys[i];
        }

        this.childPointers = new Node[maxDegree + 1];
        for (short i=0; i<node.childPointers.length; i++) {
            this.childPointers[i] = node.childPointers[i];
        }
    }

    public boolean appendKey(Mbr key, Node node) {
        if (this.isFull())
            return false;

        keys[degree] = key;
        childPointers[degree] = node;
        degree ++;

        mbr.getEnvelope().expandToInclude(key.getEnvelope());

        return true;
    }

    public void appendKeyForSplitNode(Mbr key, Node node) {
        keys[degree] = key;
        childPointers[degree] = node;
        degree++;
    }

    public boolean isFull() {
        return degree >= maxDegree;
    }
}
