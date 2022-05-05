package datamodel;

import datawriter.RDFWriter;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.IntersectionMatrix;

public class RelatedGeometries {

    private int exceptions;
    private int detectedLinks;
    private int interlinkedGeometries;
    private final int qualifyingPairs;
    private int verifiedPairs;

    private float pgr;

    private final RDFWriter rdfWriter;
    private final boolean toExport;

    private final TIntList containsSource;
    private final TIntList containsTarget;
    private final TIntList coveredBySource;
    private final TIntList coveredByTarget;
    private final TIntList coversSource;
    private final TIntList coversTarget;
    private final TIntList crossesSource;
    private final TIntList crossesTarget;
    private final TIntList equalsSource;
    private final TIntList equalsTarget;
    private final TIntList intersectsSource;
    private final TIntList intersectsTarget;
    private final TIntList overlapsSource;
    private final TIntList overlapsTarget;
    private final TIntList touchesSource;
    private final TIntList touchesTarget;
    private final TIntList withinSource;
    private final TIntList withinTarget;

    public RelatedGeometries(int qualifyingPairs) {
        pgr = 0;
        exceptions = 0;
        detectedLinks = 0;
        verifiedPairs = 0;
        this.qualifyingPairs = qualifyingPairs;
        interlinkedGeometries = 0;

        containsSource = new TIntArrayList();
        containsTarget = new TIntArrayList();
        coveredBySource = new TIntArrayList();
        coveredByTarget = new TIntArrayList();
        coversSource = new TIntArrayList();
        coversTarget = new TIntArrayList();
        crossesSource = new TIntArrayList();
        crossesTarget = new TIntArrayList();
        equalsSource = new TIntArrayList();
        equalsTarget = new TIntArrayList();
        intersectsSource = new TIntArrayList();
        intersectsTarget = new TIntArrayList();
        overlapsSource = new TIntArrayList();
        overlapsTarget = new TIntArrayList();
        touchesSource = new TIntArrayList();
        touchesTarget = new TIntArrayList();
        withinSource = new TIntArrayList();
        withinTarget = new TIntArrayList();

        rdfWriter = null;
        toExport = false;
    }

    public RelatedGeometries(int qualifyingPairs, String exportPath) {
        pgr = 0;
        exceptions = 0;
        detectedLinks = 0;
        verifiedPairs = 0;
        this.qualifyingPairs = qualifyingPairs;
        interlinkedGeometries = 0;

        containsSource = new TIntArrayList();
        containsTarget = new TIntArrayList();
        coveredBySource = new TIntArrayList();
        coveredByTarget = new TIntArrayList();
        coversSource = new TIntArrayList();
        coversTarget = new TIntArrayList();
        crossesSource = new TIntArrayList();
        crossesTarget = new TIntArrayList();
        equalsSource = new TIntArrayList();
        equalsTarget = new TIntArrayList();
        intersectsSource = new TIntArrayList();
        intersectsTarget = new TIntArrayList();
        overlapsSource = new TIntArrayList();
        overlapsTarget = new TIntArrayList();
        touchesSource = new TIntArrayList();
        touchesTarget = new TIntArrayList();
        withinSource = new TIntArrayList();
        withinTarget = new TIntArrayList();

        rdfWriter = new RDFWriter(exportPath);
        toExport = true;
    }

    private void addContains(int gId1, int gId2) {
        containsSource.add(gId1);
        containsTarget.add(gId2);
    }

    private void addCoveredBy(int gId1, int gId2) {
        coveredBySource.add(gId1);
        coveredByTarget.add(gId2);
    }

    private void addCovers(int gId1, int gId2) {
        coversSource.add(gId1);
        coversTarget.add(gId2);
    }

    private void addCrosses(int gId1, int gId2) {
        crossesSource.add(gId1);
        crossesTarget.add(gId2);
    }

    private void addEquals(int gId1, int gId2) {
        equalsSource.add(gId1);
        equalsTarget.add(gId2);
    }

    private void addIntersects(int gId1, int gId2) {
        intersectsSource.add(gId1);
        intersectsTarget.add(gId2);
    }

    private void addOverlaps(int gId1, int gId2) {
        overlapsSource.add(gId1);
        overlapsTarget.add(gId2);
    }

    private void addTouches(int gId1, int gId2) {
        touchesSource.add(gId1);
        touchesTarget.add(gId2);
    }

    private void addWithin(int gId1, int gId2) {
        withinSource.add(gId1);
        withinTarget.add(gId2);
    }

    public int getInterlinkedPairs() {
        return interlinkedGeometries;
    }

    public int getNoOfContains() {
        return containsSource.size();
    }

    public int getNoOfCoveredBy() {
        return coveredBySource.size();
    }

    public int getNoOfCovers() {
        return coversSource.size();
    }

    public int getNoOfCrosses() {
        return crossesSource.size();
    }

    public int getNoOfEquals() {
        return equalsSource.size();
    }

    public int getNoOfIntersects() {
        return intersectsSource.size();
    }

    public int getNoOfOverlaps() {
        return overlapsSource.size();
    }

    public int getNoOfTouches() {
        return touchesSource.size();
    }

    public int getNoOfWithin() {
        return withinSource.size();
    }

    public double getPrecision() {
        return interlinkedGeometries / (double) verifiedPairs;
    }
    
    public double getProgressiveGeometryRecall() {
        return pgr / qualifyingPairs / verifiedPairs;
    }    
    
    public double getRecall() {
        return interlinkedGeometries / (double) qualifyingPairs;
    }
    
    public int getQualifyingPairs() {
        return qualifyingPairs;
    }
    
    public int getVerifiedPairs() {
        return verifiedPairs;
    }

    public String print() {
        StringBuilder sb = new StringBuilder();
        sb.append("Qualifying pairs\t:\t").append(getQualifyingPairs()).append("\n");
        sb.append("Exceptions\t:\t").append(exceptions).append("\n");
        sb.append("Detected Links\t:\t").append(detectedLinks).append("\n");
        sb.append("Interlinked geometries\t:\t").append(getInterlinkedPairs()).append("\n");
        sb.append("No of contains\t:\t").append(getNoOfContains()).append("\n");
        sb.append("No of covered-by:\t").append(getNoOfCoveredBy()).append("\n");
        sb.append("No of covers\t:\t").append(getNoOfCovers()).append("\n");
        sb.append("No of crosses\t:\t").append(getNoOfCrosses()).append("\n");
        sb.append("No of equals\t:\t").append(getNoOfEquals()).append("\n");
        sb.append("No of intersects:\t").append(getNoOfIntersects()).append("\n");
        sb.append("No of overlaps\t:\t").append(getNoOfOverlaps()).append("\n");
        sb.append("No of touches\t:\t").append(getNoOfTouches()).append("\n");
        sb.append("No of within\t:\t").append(getNoOfWithin()).append("\n");
        sb.append("Recall\t:\t").append(getRecall()).append("\n");
        sb.append("Precision\t:\t").append(getPrecision()).append("\n");
        sb.append("Progressive Geometry Recall\t:\t").append(getProgressiveGeometryRecall()).append("\n");
        sb.append("Verified pairs\t:\t").append(getVerifiedPairs()).append("\n");
        return sb.toString();
    }

    public boolean verifyRelations(int sourceId, int targetId, Geometry sourceGeom, Geometry targetGeom) {
        try {
            final int dimension1 = sourceGeom.getDimension();
            final int dimension2 = targetGeom.getDimension();
            final IntersectionMatrix im = sourceGeom.relate(targetGeom);

            verifiedPairs++;
            boolean related = false;
            if (im.isContains()) {
                related = true;
                detectedLinks++;
                addContains(sourceId, targetId);
                if (toExport) rdfWriter.exportPair(sourceId, targetId, rdfWriter.getContains());
            }
            if (im.isCoveredBy()) {
                related = true;
                detectedLinks++;
                addCoveredBy(sourceId, targetId);
                if (toExport) rdfWriter.exportPair(sourceId, targetId, rdfWriter.getCoveredBy());
            }
            if (im.isCovers()) {
                related = true;
                detectedLinks++;
                addCovers(sourceId, targetId);
                if (toExport) rdfWriter.exportPair(sourceId, targetId, rdfWriter.getCovers());
            }
            if (im.isCrosses(dimension1, dimension2)) {
                related = true;
                detectedLinks++;
                addCrosses(sourceId, targetId);
                if (toExport) rdfWriter.exportPair(sourceId, targetId, rdfWriter.getCrosses());
            }
            if (im.isEquals(dimension1, dimension2)) {
                related = true;
                detectedLinks++;
                addEquals(sourceId, targetId);
                if (toExport) rdfWriter.exportPair(sourceId, targetId, rdfWriter.getEquals());
            }
            if (im.isIntersects()) {
                related = true;
                detectedLinks++;
                addIntersects(sourceId, targetId);
                if (toExport) rdfWriter.exportPair(sourceId, targetId, rdfWriter.getIntersects());
            }
            if (im.isOverlaps(dimension1, dimension2)) {
                related = true;
                detectedLinks++;
                addOverlaps(sourceId, targetId);
                if (toExport) rdfWriter.exportPair(sourceId, targetId, rdfWriter.getOverlaps());
            }
            if (im.isTouches(dimension1, dimension2)) {
                related = true;
                detectedLinks++;
                addTouches(sourceId, targetId);
                if (toExport) rdfWriter.exportPair(sourceId, targetId, rdfWriter.getTouches());
            }
            if (im.isWithin()) {
                related = true;
                detectedLinks++;
                addWithin(sourceId, targetId);
                if (toExport) rdfWriter.exportPair(sourceId, targetId, rdfWriter.getWithin());
            }

            if (related) {
                interlinkedGeometries++;
            }
            pgr += interlinkedGeometries;


            return related;
        } catch (Exception ex) {
            ex.printStackTrace();
            exceptions++;
            return false;
        }
    }

    public void close() {
        if (toExport) rdfWriter.close();
    }
}
