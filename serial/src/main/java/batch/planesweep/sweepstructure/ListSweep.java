package batch.planesweep.sweepstructure;

import datamodel.GeometryProfile;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

public class ListSweep implements SweepStructure {

    List<ListNode> list;
    GeometryProfile deleteLimit;
    boolean toDel;

    public ListSweep() {
        toDel = false;
        this.list = new ArrayList<>();
    }

    @Override
    public void insert(GeometryProfile geometryProfile, int id) {
        ListNode node = new ListNode(geometryProfile, id);
        list.add(node);
    }

    @Override
    public void removeExpired(GeometryProfile limit) {
        this.toDel = true;
        deleteLimit = limit;
    }

    @Override
    public TIntSet reportOverlap(GeometryProfile source, int sourceId) {
        final TIntSet candidates = new TIntHashSet();
        ListIterator<ListNode> iterator = list.listIterator();
        while (iterator.hasNext()) {
            ListNode current = iterator.next();
            if (toDel && current.geometryProfile.getMaxX() < deleteLimit.getMinX()) {
                iterator.remove();
            } else {
                candidates.add(current.getId());
            }
        }

        toDel = false;
        return candidates;
    }

    @Override
    public int getSize() {
        return this.list.size();
    }

    @Override
    public void setThetas(GeometryProfile[] sourceData) {

    }

}
