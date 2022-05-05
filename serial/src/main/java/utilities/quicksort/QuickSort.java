package utilities.quicksort;

import enums.Axis;
import datamodel.GeometryProfile;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class QuickSort {

    public static void quickSort(GeometryProfile[] geometryProfiles, Axis axis) {
        quickSort(geometryProfiles, 0, geometryProfiles.length - 1, axis);
    }

    public static void quickSort(GeometryProfile[] geometryProfiles, int first, int last, Axis axis) {

        if (first < last) {

            GeometryProfile median = sedgewickMedian(geometryProfiles[first], geometryProfiles[last], geometryProfiles[(first+last)/2], axis);
            int split = partition(geometryProfiles, first, last, median, axis);

            quickSort(geometryProfiles, first, split, axis);
            quickSort(geometryProfiles, split+1, last, axis);
        }
    }

    private static int partition(GeometryProfile[] geometryProfiles, int first, int last, GeometryProfile median, Axis axis) {

        int i = first - 1;
        int j = last + 1;

        while(true) {

            do {
                j = j - 1;
            } while (geometryProfiles[j].isGreaterThan(median, axis));

            do {
                i = i + 1;
            } while (geometryProfiles[i].isLessThan(median, axis));

            if (i < j)
                swap(geometryProfiles, i, j);
            else
                return j;
        }
    }

    private static void swap(GeometryProfile[] geometryProfiles, int i, int j) {
        GeometryProfile geometryProfileTemp = geometryProfiles[i];
        geometryProfiles[i] = geometryProfiles[j];
        geometryProfiles[j] = geometryProfileTemp;
    }

    private static GeometryProfile sedgewickMedian(GeometryProfile first, GeometryProfile last, GeometryProfile mid, Axis axis) {
        List<GeometryProfile> candidates = new ArrayList<>();

        candidates.add(first);
//        candidates.add(last);
        candidates.add(mid);

        return Collections
                .max(candidates, Comparator
                        .comparingDouble(o -> {
                            if (axis == Axis.X_AXIS)
                                return o.getGeometry().getEnvelopeInternal().getMinX();
                            else
                                return o.getGeometry().getEnvelopeInternal().getMinY();
                        }));
    }

}