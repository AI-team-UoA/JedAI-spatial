package batch.planesweep;

import batch.AbstractBatchAlgorithm;
import batch.planesweep.sweepstructure.ListSweep;
import batch.planesweep.sweepstructure.StripedSweep;
import batch.planesweep.sweepstructure.SweepStructure;
import datamodel.GeometryProfile;
import datareader.AbstractReader;
import enums.Axis;
import enums.PlaneSweepStructure;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.set.TIntSet;
import org.apache.jena.atlas.json.JsonArray;
import utilities.quicksort.QuickSort;

public class PlaneSweep extends AbstractBatchAlgorithm {

    protected final GeometryProfile[] targetData;
    protected final QuickSort quickSort;
    protected final SweepStructure sweepStructure;

    public PlaneSweep(int qPairs, AbstractReader sourceReader, AbstractReader targetReader, PlaneSweepStructure planeSweepStructure) {
        super(qPairs, sourceReader, targetReader);

        this.sweepStructure = planeSweepStructure == PlaneSweepStructure.LIST_SWEEP
                ? new ListSweep()
                : new StripedSweep();

        this.quickSort = new QuickSort();
        
        targetData = targetReader.getGeometryProfiles();
    }

    public PlaneSweep(int qPairs, AbstractReader sourceReader, AbstractReader targetReader,
                      PlaneSweepStructure planeSweepStructure, String exportPath) {
        super(qPairs, sourceReader, targetReader, exportPath);

        this.sweepStructure = planeSweepStructure == PlaneSweepStructure.LIST_SWEEP
                ? new ListSweep()
                : new StripedSweep();

        this.quickSort = new QuickSort();

        targetData = targetReader.getGeometryProfiles();
    }

    @Override
    protected void verification() {
        int sourceIndex = 0;
        int sourceEndIndex = sourceData.length;

        int targetIndex = 0;
        int targetEndIndex = targetData.length;

        while (sourceIndex < sourceEndIndex) {
            if (targetIndex == targetEndIndex) {
                break;
            }

            GeometryProfile source = sourceData[sourceIndex];
            GeometryProfile candidate = targetData[targetIndex];
            if (candidate == null || source.getMaxX() < candidate.getMinX()) {
                sweepStructure.removeExpired(source);

                final TIntSet partialCandidates = sweepStructure.reportOverlap(source, sourceIndex);
                final TIntIterator intIterator = partialCandidates.iterator();
                while (intIterator.hasNext()) {
                    int candidateMatchId = intIterator.next();
                    if (targetData[candidateMatchId].getGeometry().getEnvelopeInternal().intersects(source.getGeometry().getEnvelopeInternal())) {
                        relations.verifyRelations(sourceIndex, candidateMatchId, source.getGeometry(), targetData[candidateMatchId].getGeometry());
                    }
                }
                sourceIndex++;
            } else {
                sweepStructure.insert(candidate, targetIndex);
                targetIndex++;
            }
        }

        while (sourceIndex < sourceEndIndex) { //&& sweepStructure.getSize() > 0) {
            GeometryProfile source = sourceData[sourceIndex];
            sweepStructure.removeExpired(source);
            final TIntSet partialCandidates = sweepStructure.reportOverlap(source, sourceIndex);
            final TIntIterator intIterator = partialCandidates.iterator();
            while (intIterator.hasNext()) {
                int candidateMatchId = intIterator.next();
                if (targetData[candidateMatchId].getGeometry().getEnvelopeInternal().intersects(source.getGeometry().getEnvelopeInternal())) {
                    relations.verifyRelations(sourceIndex, candidateMatchId, source.getGeometry(), targetData[candidateMatchId].getGeometry());
                }
            }
            sourceIndex++;
        }

        relations.close();
    }

    @Override
    protected void filtering() {
        sweepStructure.setThetas(sourceData);
        QuickSort.quickSort(sourceData, Axis.X_AXIS);
        QuickSort.quickSort(targetData, Axis.X_AXIS);
    }

    @Override
    public String getMethodConfiguration() {
        return null;
    }

    @Override
    public String getMethodInfo() {
        return null;
    }

    @Override
    public String getMethodName() {
        return "Plane Sweep";
    }

    @Override
    public String getMethodParameters() {
        return null;
    }

    @Override
    public JsonArray getParameterConfiguration() {
        return null;
    }

    @Override
    public String getParameterDescription(int parameterId) {
        return null;
    }

    @Override
    public String getParameterName(int parameterId) {
        return null;
    }
}
