package datamodel.crtree;

import datamodel.GeometryProfile;
import gnu.trove.list.TIntList;
import lombok.Getter;
import org.locationtech.jts.geom.Envelope;
import datamodel.Mbr;

@Getter
public class Node {

    private final int QLEVEL;
    private final boolean isLeaf;
    private int count;
    private final Mbr referenceMBR;

    private final Mbr[] mbrs;
    private final Node[] childPointers;

    public Node(int max, Mbr referenceMBR, int QLEVEl, boolean isLeaf) {
        this.count = 0;
        this.QLEVEL = QLEVEl;
        this.isLeaf = isLeaf;
        this.referenceMBR = referenceMBR;

        mbrs = new Mbr[max];
        childPointers = new Node[max];
    }

    public int phi1(double a, double b, double r, int QLEVEL) {
        if (r <= a) {
            return 0;
        } else if (r >= b) {
            return QLEVEL - 1;
        } else {
            return (int) Math.floor(QLEVEL * ((r - a)/(b - a)));
        }
    }

    public int phi2(double a, double b, double r, int QLEVEL) {
        if (r <= a) {
            return 0;
        } else if (r >= b) {
            return QLEVEL;
        } else {
            return (int) Math.ceil(QLEVEL * ((r - a)/(b - a)));
        }
    }

    public Mbr computeQRMBR(Mbr ref, Mbr r, int QLEVEL) {
        Mbr qrmbr = new Mbr();
        qrmbr.setIndex(r.getIndex());

        Envelope env = new Envelope(
                phi1(ref.getMinX(), ref.getMaxX(), r.getMinX(), QLEVEL),
                phi2(ref.getMinX(), ref.getMaxX(), r.getMaxX(), QLEVEL),
                phi1(ref.getMinY(), ref.getMaxY(), r.getMinY(), QLEVEL),
                phi2(ref.getMinY(), ref.getMaxY(), r.getMaxX(), QLEVEL));

        qrmbr.setEnvelope(env);
        return qrmbr;
    }

    public Mbr computeQRMBR(Mbr ref, Envelope r, int QLEVEL) {
        Mbr qrmbr = new Mbr();
        Envelope env = new Envelope(
                phi1(ref.getMinX(), ref.getMaxX(), r.getMinX(), QLEVEL),
                phi2(ref.getMinX(), ref.getMaxX(), r.getMaxX(), QLEVEL),
                phi1(ref.getMinY(), ref.getMaxY(), r.getMinY(), QLEVEL),
                phi2(ref.getMinY(), ref.getMaxY(), r.getMaxX(), QLEVEL));

        qrmbr.setEnvelope(env);
        return qrmbr;
    }

    public void addEntry(Mbr qrmbr, GeometryProfile geometryProfile) {
        mbrs[count] = qrmbr;
        count ++;
    }

    public void addEntry(Mbr qrmbr, Node child) {
        mbrs[count] = qrmbr;
        childPointers[count] = child;
        count ++;
    }

    public void traverseTree(Envelope candidate, TIntList geometryIndexes) {

        Mbr qrmbr = this.computeQRMBR(this.referenceMBR, candidate, QLEVEL);
        if (isLeaf) {
            for (short i = 0; i < count; i++) {
                    geometryIndexes.add(mbrs[i].getIndex());
            }
        } else {
            for (short i = 0; i < count; i++) {
                if (mbrs[i].getEnvelope().intersects(qrmbr.getEnvelope()))
                    childPointers[i].traverseTree(candidate, geometryIndexes);
            }
        }
    }
}
