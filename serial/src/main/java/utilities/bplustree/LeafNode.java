package utilities.bplustree;

import java.util.Arrays;

public class LeafNode extends Node {
    int maxNumPairs;
    int minNumPairs;
    int numPairs;
    LeafNode leftSibling;
    LeafNode rightSibling;
    DictionaryPair[] dictionary;

    protected void delete(int index) {
        this.dictionary[index] = null;
        numPairs--;
    }

    protected boolean insert(DictionaryPair dp) {
        if (this.isFull()) {
            return false;
        } else {
            this.dictionary[numPairs] = dp;
            numPairs++;
            Arrays.sort(this.dictionary, 0, numPairs);

            return true;
        }
    }

    protected boolean isDeficient() {
        return numPairs < minNumPairs;
    }

    protected boolean isFull() {
        return numPairs == maxNumPairs;
    }

    protected boolean isLendable() {
        return numPairs > minNumPairs;
    }

    protected boolean isMergeable() {
        return numPairs == minNumPairs;
    }

    protected LeafNode(BPlusTree bPlusTree, int m, DictionaryPair dp) {
        this.maxNumPairs = m - 1;
        this.minNumPairs = (int) (Math.ceil(m / 2) - 1);
        this.dictionary = new DictionaryPair[m];
        this.numPairs = 0;
        this.insert(dp);
    }

    protected LeafNode(BPlusTree bPlusTree, int m, DictionaryPair[] dps, InternalNode parent) {
        this.maxNumPairs = m - 1;
        this.minNumPairs = (int) (Math.ceil(m / 2) - 1);
        this.dictionary = dps;
        this.numPairs = bPlusTree.linearNullSearch(dps);
        this.parent = parent;
    }
}
