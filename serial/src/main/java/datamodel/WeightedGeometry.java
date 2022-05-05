package datamodel;

import lombok.Getter;

@Getter
public class WeightedGeometry implements Comparable<WeightedGeometry> {
    
    private final int geometryId;
    private final float averageWeight;
    
    public WeightedGeometry(int id, float avWeight) {
        geometryId = id;
        averageWeight = avWeight;
    }

    @Override
    public int compareTo(WeightedGeometry o) {
        float avWeigt2 = o.getAverageWeight();
        if (averageWeight < avWeigt2) {
            return 1;
        } else if (avWeigt2 < averageWeight) {
            return -1;
        }
        return 0;
    }
}
