package batch.planesweep.sweepstructure;

import datamodel.GeometryProfile;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import org.locationtech.jts.geom.Envelope;

public class StripedSweep implements SweepStructure {

    ListSweep[] strips;
    double minY, maxY;
    double stripSize;
    boolean toDel;
    int size;

    public StripedSweep() {
        this.stripSize = 0.0;
        this.minY = Integer.MAX_VALUE;
        this.maxY = Integer.MIN_VALUE;
        toDel = false;
    }

    public StripedSweep(int numOfStrips, double maxY, double minY) {
        this.strips = new ListSweep[numOfStrips];
        this.minY = minY;
        this.maxY = maxY;
        this.stripSize = (maxY - minY) / strips.length;
        toDel = false;
    }

    @Override
    public void insert(GeometryProfile geometryProfile, int id) {
        if (geometryProfile.getMinY() > maxY || geometryProfile.getMaxY() < minY) {
            return;
        }

        int maxY = (int) Math.ceil((geometryProfile.getMaxY() - this.minY) / stripSize);
        int minY = (int) Math.floor((geometryProfile.getMinY() - this.minY) / stripSize);
        for (int i = minY; i < maxY; i++) {
            strips[i].insert(geometryProfile, id);
            size++;
        }
    }

    @Override
    public void removeExpired(GeometryProfile limit) {
        int maxY = (int) Math.ceil((limit.getMaxY() - this.minY) / stripSize);
        int minY = (int) Math.floor((limit.getMinY() - this.minY) / stripSize);
        for (int i = minY; i < maxY; i++) {
            strips[i].removeExpired(limit);
        }
    }

    @Override
    public TIntSet reportOverlap(GeometryProfile candidate, int candidateId) {
        final TIntSet candidates = new TIntHashSet();

        int maxY = (int) Math.ceil((candidate.getMaxY() - this.minY) / stripSize);
        int minY = (int) Math.floor((candidate.getMinY() - this.minY) / stripSize);
        for (int i = minY; i < maxY; i++) {
            candidates.addAll(strips[i].reportOverlap(candidate, candidateId));
            size--;
        }

        return candidates;
    }

    @Override
    public int getSize() {
        return this.size;
    }

    @Override
    public void setThetas(GeometryProfile[] sourceData) {
        for (GeometryProfile profile : sourceData) {
            final Envelope en = profile.getGeometry().getEnvelopeInternal();
            stripSize += en.getWidth();
            maxY = Math.max(maxY, en.getMaxY());
            minY = Math.min(minY, en.getMinY());
        }

        minY -= 1.0;
        maxY += 1.0;

        stripSize /= sourceData.length;
        strips = new ListSweep[(int) (Math.ceil((maxY - minY) / stripSize))];
        for (int i = 0; i < strips.length; i++) {
            strips[i] = new ListSweep();
        }
    }

}
