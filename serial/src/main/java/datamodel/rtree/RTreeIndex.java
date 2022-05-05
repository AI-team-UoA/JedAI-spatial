package datamodel.rtree;

import datamodel.GeometryProfile;
import datamodel.Mbr;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import lombok.Getter;
import lombok.Setter;
import org.locationtech.jts.geom.Envelope;

/**
 * Antonin Guttman R Tree implementation Used for Geometry indexing
 */
@Getter
@Setter
public class RTreeIndex {

    private int m;
    private Node root;

    public RTreeIndex() {
        this.m = 6;
        this.root = new LeafNode(m);
    }

    /**
     * Inserting index records for new data tuples is similar to insertion in a
     * B-tree in that new index records are added to the leaves, nodes that
     * overflow are split, and splits propagate up the tree
     *
     * @param geometryProfile: geometry to add
     */
    public void insert(GeometryProfile geometryProfile, int index) {

        LeafNode leaf = this.chooseLeaf(geometryProfile, this.root);

        Mbr mbr = new Mbr(geometryProfile.getGeometry().getEnvelopeInternal(), index);
        boolean isNotFull = leaf.appendKey(mbr, null);

        LeafNode[] splitNodes = null;
        if (!isNotFull) {
            LeafNode leafNode = new LeafNode(this.m);
            leafNode.setMbr(mbr);
            Node[] temp = this.splitNode(leaf, leafNode);

            splitNodes = new LeafNode[2];
            splitNodes[0] = new LeafNode(temp[0]);
            splitNodes[1] = new LeafNode(temp[1]);

            leaf.setKeys(temp[0].getKeys());
            leaf.setMbr(temp[0].getMbr());
            leaf.setDegree(temp[0].getDegree());
        }

        this.adjustTree(leaf, splitNodes);

//        this.printTree(this.root, 0);
//        System.out.print("\n\n");
    }

    /**
     * Ascend from a leaf node L to the root, adjusting covering rectangles and
     * propagating node splits as necessary
     *
     * @param node: node
     * @param groups: split happened in previous level
     */
    private void adjustTree(Node node, Node[] groups) {

        // return if we propagated to root
        if (node.getParent() == null) {

            if (groups != null) {

                InternalNode newRoot = new InternalNode(this.m);

                newRoot.appendKey(node.getMbr(), node);
                newRoot.appendKey(groups[1].getMbr(), groups[1]);

                node.setParent(newRoot);
                groups[1].setParent(newRoot);

                this.root = newRoot;
            }

            return;
        }

        // adjust covering rectangle in parent entry
        InternalNode parent = node.getParent();

        int childIndex = -1;
        for (short i = 0; i < parent.getDegree(); i++) {
            if (parent.getChildPointers()[i] == node) {
                childIndex = i;
            }
        }

        // propagate node split upward
        parent.getMbr().getEnvelope().expandToInclude(node.getMbr().getEnvelope());
        parent.getKeys()[childIndex] = node.getMbr();
        parent.getChildPointers()[childIndex] = node;

        // if split happened at an earlier level
        InternalNode[] splitNodes = null;
        if (groups != null) {
            boolean isNotFull = parent.appendKey(groups[1].getMbr(), groups[1]);

            if (!isNotFull) {
                splitNodes = this.splitNode(parent, groups[1]);

//                parent = splitNodes[0];
                parent.setMbr(splitNodes[0].getMbr());
                parent.setChildPointers(splitNodes[0].getChildPointers());
                parent.setKeys(splitNodes[0].getKeys());
                parent.setDegree(splitNodes[0].getDegree());
            } else {
                groups[0].setParent(parent);

                parent.getMbr().getEnvelope().expandToInclude(groups[1].getMbr().getEnvelope());
                groups[1].setParent(parent);
            }
        }

        this.adjustTree(parent, splitNodes);
    }

    /**
     * Select a leaf node in which to place a new index entry E
     *
     * @param geometry: geometry to add
     * @param node_: target node
     * @return target LeafNode
     */
    private LeafNode chooseLeaf(GeometryProfile geometry, Node node_) {

        // Leaf Check
        if (node_ instanceof LeafNode) {
            return (LeafNode) node_;
        }

        InternalNode node = new InternalNode(node_);

        int bestSubtree = -1;
        double bestExpansion = Double.MAX_VALUE;

        // Get the subtree which expands the least
        for (int i = 0; i < node.getDegree(); i++) {
            Mbr currentMbr = node.getKeys()[i];
            Envelope expanded = new Envelope(currentMbr.getEnvelope());
            expanded.expandToInclude(geometry.getGeometry().getEnvelopeInternal());

            double expansion = expanded.getArea() - currentMbr.getEnvelope().getArea();

            // get the node whose mbr expands the least
            if (expansion < bestExpansion) {
                bestSubtree = i;
                bestExpansion = expansion;
            } else if (expansion == bestExpansion) {

                // if there is a tie get the mbr with smallest area
                double currentArea = currentMbr.getEnvelope().getArea();
                double bestArea = node.getKeys()[bestSubtree].getEnvelope().getArea();

                if (currentArea < bestArea) {
                    bestSubtree = i;
                    bestExpansion = expansion;
                }
            }
        }

        // descend until a leaf is reached
        return chooseLeaf(geometry, node.getChildPointers()[bestSubtree]);
    }

    /**
     * Quadratic Split Algorithm Divide a set of M + 1 index entries into two
     * groups
     *
     * @param node_: parent node
     * @param mbr: node to add
     * @return array of Nodes
     */
    private InternalNode[] splitNode(Node node_, Node mbr) {

        int maxPair1Index = -1;
        int maxPair2Index = -1;

        double maxScore = Integer.MIN_VALUE;

        /**
         * Quadratic PickSeeds Choose the most wasteful pair
         */
        Node node = new Node(node_);
        if (node_ instanceof LeafNode) {
            node.appendKeyForSplitNode(mbr.getMbr(), null);
        } else {
            node.appendKeyForSplitNode(mbr.getMbr(), mbr);
        }

        for (short i = 0; i < node.getDegree(); i++) {
            for (short j = 0; j < node.getDegree(); j++) {

                if (i == j) {
                    continue;
                }

                Mbr mbrI = node.getKeys()[i];
                Mbr mbrJ = node.getKeys()[j];
                Envelope expanded = new Envelope(mbrI.getEnvelope());
                expanded.expandToInclude(mbrJ.getEnvelope());

                double score
                        = expanded.getArea()
                        - mbrI.getEnvelope().getArea()
                        - mbrJ.getEnvelope().getArea();

                if (score > maxScore) {
                    maxScore = score;
                    maxPair1Index = i;
                    maxPair2Index = j;
                }
            }
        }

        InternalNode group1 = new InternalNode(this.m);
        InternalNode group2 = new InternalNode(this.m);

        // append to groups
        group1.appendKey(node.getKeys()[maxPair1Index], node.getChildPointers()[maxPair1Index]);
        group2.appendKey(node.getKeys()[maxPair2Index], node.getChildPointers()[maxPair2Index]);

        if (node_ instanceof InternalNode) {
            node.getChildPointers()[maxPair1Index].setParent((InternalNode) node_);
            node.getChildPointers()[maxPair2Index].setParent(group2);
        }

        /**
         * PickNext Select one remaining entry for classification in a group
         */
        Envelope group1Mbr = new Envelope(node.getKeys()[maxPair1Index].getEnvelope());
        Envelope group2Mbr = new Envelope(node.getKeys()[maxPair2Index].getEnvelope());
        for (short i = 0; i < node.getDegree(); i++) {

            if (maxPair1Index == i || maxPair2Index == i) {
                continue;
            }

            Envelope expandedGroup1Mbr = new Envelope(group1Mbr);
            expandedGroup1Mbr.expandToInclude(node.getKeys()[i].getEnvelope());
            double d1 = expandedGroup1Mbr.getArea() - group1Mbr.getArea();

            Envelope expandedGroup2Mbr = new Envelope(group2Mbr);
            expandedGroup2Mbr.expandToInclude(node.getKeys()[i].getEnvelope());
            double d2 = expandedGroup2Mbr.getArea() - group2Mbr.getArea();

            if (d1 > d2) {
                group1Mbr = expandedGroup1Mbr;
                group1.appendKey(node.getKeys()[i], node.getChildPointers()[i]);

                if (node_ instanceof InternalNode) {
                    node.getChildPointers()[i].setParent((InternalNode) node_);
                }

            } else {
                group2Mbr = expandedGroup2Mbr;
                group2.appendKey(node.getKeys()[i], node.getChildPointers()[i]);

                if (node_ instanceof InternalNode) {
                    node.getChildPointers()[i].setParent(group2);
                }
            }
        }

        return new InternalNode[]{group1, group2};
    }

    public TIntList rangeSearch(Envelope envelope) {
        TIntList geometryIndexes = new TIntArrayList();
        this.traverseTree(this.root, envelope, geometryIndexes);

        return geometryIndexes;
    }

    public void traverseTree(Node node, Envelope envelope, TIntList geometryIndexes) {

        if (node instanceof LeafNode) {
            for (short i = 0; i < node.getDegree(); i++) {
                geometryIndexes.add(node.getKeys()[i].getIndex());
            }

            return;
        }

        if (node.getMbr().getEnvelope().intersects(envelope)) {
            for (short i = 0; i < node.getDegree(); i++) {
                this.traverseTree(node.getChildPointers()[i], envelope, geometryIndexes);
            }
        }
    }

    public void printTree(Node node, int counter) {
        counter++;
        System.out.print("-");
        for (short j = 0; j < counter; j++) {
            System.out.print("-");
        }

        if (counter == 1) {
            System.out.println("ROOT");
        } else {
            System.out.println(node.getClass().getName());
        }
        for (short i = 0; i < node.getDegree(); i++) {

            System.out.print("-");
            for (short j = 0; j < counter; j++) {
                System.out.print("-");
            }

            if (node instanceof InternalNode) {
                this.printTree(node.getChildPointers()[i], counter);
            } else {
                System.out.println(node.getKeys()[i].getIndex());
            }

        }
    }

}
