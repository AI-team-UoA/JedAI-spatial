package batch.planesweep.sweepstructure;

import datamodel.GeometryProfile;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ListNode {

    ListNode next, prev;
    GeometryProfile geometryProfile;
    int id;

    public ListNode(GeometryProfile geometryProfile, int id) {
        this.geometryProfile = geometryProfile;
        this.id = id;
        this.next = null;
        this.prev = null;
    }
}
