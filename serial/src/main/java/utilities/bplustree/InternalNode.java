package utilities.bplustree;

class InternalNode extends Node {
    int maxDegree;
    int minDegree;
    int degree;
    InternalNode leftSibling;
    InternalNode rightSibling;
    Integer[] keys;
    Node[] childPointers;

    protected void appendChildPointer(Node pointer) {
        this.childPointers[degree] = pointer;
        this.degree++;
    }

    protected int findIndexOfPointer(Node pointer) {
        for (int i = 0; i < childPointers.length; i++) {
            if (childPointers[i] == pointer) {
                return i;
            }
        }
        return -1;
    }

    protected void insertChildPointer(Node pointer, int index) {
        for (int i = degree - 1; i >= index; i--) {
            childPointers[i + 1] = childPointers[i];
        }
        this.childPointers[index] = pointer;
        this.degree++;
    }

    protected boolean isDeficient() {
        return this.degree < this.minDegree;
    }

    protected boolean isLendable() {
        return this.degree > this.minDegree;
    }

    protected boolean isMergeable() {
        return this.degree == this.minDegree;
    }

    protected boolean isOverfull() {
        return this.degree == maxDegree + 1;
    }

    protected void prependChildPointer(Node pointer) {
        for (int i = degree - 1; i >= 0; i--) {
            childPointers[i + 1] = childPointers[i];
        }
        this.childPointers[0] = pointer;
        this.degree++;
    }

    protected void removeKey(int index) {
        this.keys[index] = null;
    }

    protected void removePointer(int index) {
        this.childPointers[index] = null;
        this.degree--;
    }

    protected void removePointer(Node pointer) {
        for (int i = 0; i < childPointers.length; i++) {
            if (childPointers[i] == pointer) {
                this.childPointers[i] = null;
            }
        }
        this.degree--;
    }

    protected InternalNode(BPlusTree bPlusTree, int m, Integer[] keys) {
        this.maxDegree = m;
        this.minDegree = (int) Math.ceil(m / 2.0);
        this.degree = 0;
        this.keys = keys;
        this.childPointers = new Node[this.maxDegree + 1];
    }

    protected InternalNode(BPlusTree bPlusTree, int m, Integer[] keys, Node[] pointers) {
        this.maxDegree = m;
        this.minDegree = (int) Math.ceil(m / 2.0);
        this.degree = bPlusTree.linearNullSearch(pointers);
        this.keys = keys;
        this.childPointers = pointers;
    }
}
