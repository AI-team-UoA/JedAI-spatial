package datamodel;

import java.io.Serializable;
import org.locationtech.jts.geom.Geometry;

public class Pair implements Serializable {

    private static final long serialVersionUID = 723425435776147L;

    private final int entityId1;
    private final int entityId2;
    private float weight;

    private final Geometry targetGeometry;
    
    public Pair (int id1, int id2, float w, Geometry tG) {
        weight = w;
        entityId1 = id1;
        entityId2 = id2;
        targetGeometry = tG;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Pair other = (Pair) obj;
        if (this.entityId1 != other.getEntityId1()) {
            return false;
        }
        return this.entityId2 == other.getEntityId2();
    }
    
    public int getEntityId1() {
        return entityId1;
    }

    public int getEntityId2() {
        return entityId2;
    }

    /**
     * @return the targetGeometry
     */
    public Geometry getTargetGeometry() {
        return targetGeometry;
    }
    
    /**
     * Returns the weight between two geometries.
     * Higher weights correspond to stronger likelihood of related entities.
     * @return 
     */
    public float getWeight() {
        return weight;
    }
    
    @Override
    public int hashCode() {
        int hash = 7;
        hash = 61 * hash + this.entityId1;
        hash = 61 * hash + this.entityId2;
        return hash;
    }
    
    public void setWeight(float w) {
        weight = w;
    }
    
    @Override
    public String toString() {
        return "E1 : " + entityId1 + ", E2 : " + entityId2 + ", weight : " + weight;
    }
}