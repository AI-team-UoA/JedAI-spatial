package batch.partitionbased;

import datamodel.GeometryProfile;
import java.util.Comparator;

public class GeometryComparator implements Comparator<GeometryProfile> {

    @Override
    public int compare(GeometryProfile g1, GeometryProfile g2) {
        Double minX1 = g1.getGeometry().getEnvelopeInternal().getMinX();
        return minX1.compareTo(g2.getGeometry().getEnvelopeInternal().getMinX());
    }
    
}
