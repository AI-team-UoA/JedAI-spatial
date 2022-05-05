package datamodel.crtree;

import datamodel.GeometryProfile;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import org.locationtech.jts.geom.Envelope;
import datamodel.Mbr;
import enums.Axis;
import utilities.quicksort.QuickSort;

import java.util.ArrayList;
import java.util.List;

/**
 * maxEntries = 4,  qlevel = 256 -> avg(vt)  =  13,655.8 (5 executions)
 * maxEntries = 8,  qlevel = 256 -> avg(vt)  =  13,501.4 (5 executions)
 * maxEntries = 16, qlevel = 256 -> avg(vt)  =  13,501.8 (5 executions)
 * maxEntries = 32, qlevel = 256 -> avg(vt)  =  14,034   (5 executions)
 *
 * maxEntries = 4,  qlevel = 512 -> avg(vt)  = 13,910    (5 executions)
 * maxEntries = 8,  qlevel = 512 -> avg(vt)  = 13,554.8  (5 executions)
 * maxEntries = 16, qlevel = 512 -> avg(vt)  = 13,254.6  (5 executions)
 * maxEntries = 32, qlevel = 512 -> avg(vt)  = 14,145    (5 executions)
 *
 * maxEntries = 4,  qlevel = 1024 -> avg(vt) = 12,945.8  (5 executions)
 * maxEntries = 8,  qlevel = 1024 -> avg(vt) = 12,674.2  (5 executions)
 * maxEntries = 16, qlevel = 1024 -> avg(vt) = 13,052.2  (5 executions)
 * maxEntries = 32, qlevel = 1024 -> avg(vt) = 12,895.6  (5 executions)
 */
public class CRTreeIndex {

    private Node root;
    private final int maxEntries;
    private final double fillFactor;
    private final int QLEVEL;

    public CRTreeIndex() {
        this.maxEntries = 6;
        this.fillFactor = 1.0;
        this.QLEVEL = 1024;
    }

    /***
     * Sort Tile Recursive Bulk Loading
     * @param sourceData
     */
    public void bulkLoad(GeometryProfile[] sourceData) {
        Node leaf;

        int numInLeaf = (int) (fillFactor * maxEntries);
        int P = (int) Math.ceil(sourceData.length / numInLeaf);
        int S = (int) Math.sqrt(P);

        QuickSort.quickSort(sourceData, Axis.X_AXIS);

        int Sn = S * numInLeaf;

        int i = 0;
        for (; i < sourceData.length - Sn; i += Sn)
            QuickSort.quickSort(sourceData, i, i + Sn, Axis.Y_AXIS);

        QuickSort.quickSort(sourceData, Axis.Y_AXIS);

        List<Node> tempNodes = new ArrayList<>();
        i = 0;
        for (; i < sourceData.length - numInLeaf; i += numInLeaf) {
            Mbr referenceMBR = new Mbr();
            referenceMBR.setIndex(i);
            referenceMBR.setEnvelope(sourceData[i].getGeometry().getEnvelopeInternal());

            for (int k = i; k < i + numInLeaf; k++) {
                Mbr key = new Mbr();
                key.setIndex(k);
                key.setEnvelope(sourceData[k].getGeometry().getEnvelopeInternal());

                referenceMBR.getEnvelope().expandToInclude(key.getEnvelope());
            }

            leaf = new Node(maxEntries, referenceMBR, QLEVEL, true);

            for (int k = i; k < i + numInLeaf; k++) {
                Mbr key = new Mbr();
                key.setIndex(k);
                key.setEnvelope(sourceData[k].getGeometry().getEnvelopeInternal());

                Mbr qrmbr = leaf.computeQRMBR(referenceMBR, key, QLEVEL);
                leaf.addEntry(qrmbr, sourceData[k]);
            }

            tempNodes.add(leaf);
        }

        // last leaf iteration
        Mbr referenceMBR = new Mbr();
        referenceMBR.setIndex(i);
        referenceMBR.setEnvelope(sourceData[i].getGeometry().getEnvelopeInternal());

        boolean added = false;
        for (int k = i + 1; k < sourceData.length; k++) {
            Mbr key = new Mbr();
            key.setIndex(k);
            key.setEnvelope(sourceData[k].getGeometry().getEnvelopeInternal());

            referenceMBR.getEnvelope().expandToInclude(key.getEnvelope());
        }

        leaf = new Node(maxEntries, referenceMBR, QLEVEL, true);

        for (int k = i; k < sourceData.length; k++) {
            Mbr key = new Mbr();
            key.setIndex(k);
            key.setEnvelope(sourceData[k].getGeometry().getEnvelopeInternal());

            Mbr qrmbr = leaf.computeQRMBR(referenceMBR, key, QLEVEL);
            leaf.addEntry(qrmbr, sourceData[k]);

            added = true;
        }

        if (added) {
            tempNodes.add(leaf);
        }

        build(tempNodes);
    }

    public void build(List<Node> internalNodes) {
        Node node;

        if (internalNodes.size() == 1) {
            root = internalNodes.get(0);
            return;
        }

        List<Node> tempNodes = new ArrayList<>();
        int numInNode = (int) (fillFactor * maxEntries);

        int i = 0;
        for (; i < internalNodes.size() - numInNode; i += numInNode) {
            Mbr rMBR = internalNodes.get(i).getReferenceMBR();

            for (int k = i + 1; k < i + numInNode; k++) {
                rMBR.getEnvelope().expandToInclude(internalNodes.get(k).getReferenceMBR().getEnvelope());
            }

            node = new Node(maxEntries, rMBR, QLEVEL, false);

            for (int k = i; k < i + numInNode; k++) {
                Mbr ref = internalNodes.get(k).getReferenceMBR();
                Mbr qrmbr = node.computeQRMBR(rMBR, ref, QLEVEL);
                node.addEntry(qrmbr, internalNodes.get(k));
            }

            tempNodes.add(node);
        }

        // last node iteration
        boolean added = false;
        Mbr rMBR = internalNodes.get(i).getReferenceMBR();
        for (int k = i + 1; k < internalNodes.size(); k++) {
            rMBR.getEnvelope().expandToInclude(internalNodes.get(k).getReferenceMBR().getEnvelope());
        }

        node = new Node(maxEntries, rMBR, QLEVEL, false);

        for (int k = i; k < internalNodes.size(); k++) {
            Mbr ref = internalNodes.get(k).getReferenceMBR();
            Mbr qrmbr = node.computeQRMBR(rMBR, ref, QLEVEL);
            node.addEntry(qrmbr, internalNodes.get(k));
            added = true;
        }

        if (added) {
            tempNodes.add(node);
        }

        build(tempNodes);
    }

    public TIntList rangeSearch(Envelope candidate) {
        TIntList geometryIndexes = new TIntArrayList();
        root.traverseTree(candidate, geometryIndexes);

        return geometryIndexes;
    }
}
