package batch.planesweep.sweepstructure;

import datamodel.GeometryProfile;
import gnu.trove.set.TIntSet;

public interface SweepStructure {

    /**
     * Inserts object of type GeometryProfile to the corresponding Sweep Structure
     * (ListSweep or StripSweep)
     * @param geometryProfile
     */
    void insert(GeometryProfile geometryProfile, int id);

    void removeExpired(GeometryProfile limit);

    TIntSet reportOverlap(GeometryProfile candidate, int candidateId);

    int getSize();

    void setThetas(GeometryProfile[] sourceData);
}
