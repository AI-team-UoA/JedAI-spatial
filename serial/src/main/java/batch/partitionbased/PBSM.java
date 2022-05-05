package batch.partitionbased;

import batch.AbstractBatchAlgorithm;
import batch.planesweep.sweepstructure.ListSweep;
import batch.planesweep.sweepstructure.StripedSweep;
import batch.planesweep.sweepstructure.SweepStructure;
import datamodel.GeometryProfile;
import datareader.AbstractReader;
import enums.PlaneSweepStructure;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.set.TIntSet;
import org.apache.jena.atlas.json.JsonArray;
import org.locationtech.jts.geom.Envelope;

import java.util.Arrays;
import org.locationtech.jts.geom.Geometry;

public class PBSM extends AbstractBatchAlgorithm {

    private final int tileDimX;
    private final int tileDimY;

    private double coverXWidth;
    private double coverYWidth;

    private Envelope cover;
    private final GeometryProfile[] targetData;
    private final PlaneSweepStructure planeSweepStructure;
    private TIntList[][] sourcePartitions;
    private TIntList[][] targetPartitions;
    
    public PBSM(int qPairs, AbstractReader sourceReader, AbstractReader targetReader, PlaneSweepStructure psStructure) {
        this(qPairs, 6, 6, sourceReader, targetReader, psStructure);
    }

    public PBSM(int qPairs, int xTiles, int yTiles, AbstractReader sourceReader, AbstractReader targetReader, PlaneSweepStructure psStructure) {
        super(qPairs, sourceReader, targetReader);

        tileDimX = xTiles;
        tileDimY = yTiles;

        planeSweepStructure = psStructure;
        targetData = targetReader.getGeometryProfiles();
    }

    public PBSM(int qPairs, AbstractReader sourceReader, AbstractReader targetReader,
                PlaneSweepStructure psStructure, String exportPath) {
        this(qPairs, 6, 6, sourceReader, targetReader, psStructure, exportPath);
    }

    public PBSM(int qPairs, int xTiles, int yTiles, AbstractReader sourceReader, AbstractReader targetReader,
                PlaneSweepStructure psStructure, String exportPath) {
        super(qPairs, sourceReader, targetReader, exportPath);

        tileDimX = xTiles;
        tileDimY = yTiles;

        planeSweepStructure = psStructure;
        targetData = targetReader.getGeometryProfiles();
    }

    /// Helprt function to assign each geometry to a partition.
    private void assignToPartition(GeometryProfile[] data, TIntList[][] partitions) {
        // sort source geometries based on MinX value
        Arrays.sort(data, new GeometryComparator());

        for (int i = 0; i < data.length; i++) {
            Envelope g = data[i].getGeometry().getEnvelopeInternal();

            int yMax = (int) Math.ceil((g.getMaxY() - cover.getMinY()) / coverYWidth);
            int yMin = (int) Math.floor((g.getMinY() - cover.getMinY()) / coverYWidth);
            int xMax = (int) Math.ceil((g.getMaxX() - cover.getMinX()) / coverXWidth);
            int xMin = (int) Math.floor((g.getMinX() - cover.getMinX()) / coverXWidth);

            for (int x = xMin; x <= xMax; x++) {
                for (int y = yMin; y <= yMax; y++) {
                    partitions[x][y].add(i);
                }
            }
        }
    }

    /// Combines the MBRS of two geometries.
    private Envelope combineMBR(Envelope g1, Envelope g2) {
        double maxx = Math.max(g1.getMaxX(), g2.getMaxX());
        double minx = Math.min(g1.getMinX(), g2.getMinX());
        double maxy = Math.max(g1.getMaxY(), g2.getMaxY());
        double miny = Math.min(g1.getMinY(), g2.getMinY());
        return new Envelope(maxx, minx, maxy, miny);
    }

    /// Computes the MBR of given geometries.
    private Envelope computeCover(GeometryProfile[] data) {
        Envelope localCover = data[0].getGeometry().getEnvelopeInternal();
        for (int i = 1; i < data.length; ++i) {
            Envelope r = data[i].getGeometry().getEnvelopeInternal();
            localCover = combineMBR(localCover, r);
        }
        return localCover;
    }

    @Override
    /// Filtering method spits the base area into partitions, and assigns each
    /// source and target geometry to the corresponding partition.
    public void filtering() {
        // we can also partition 'this.cover' instead of joining the two MBRs.
        cover = combineMBR(computeCover(sourceData), computeCover(targetData));

        // create partitions
        coverXWidth = (cover.getMaxX() - cover.getMinX()) / tileDimX;
        coverYWidth = (cover.getMaxY() - cover.getMinY()) / tileDimY;
        sourcePartitions = new TIntArrayList[tileDimX + 1][tileDimY + 1];
        targetPartitions = new TIntArrayList[tileDimX + 1][tileDimY + 1];
        for (int i = 0; i <= tileDimX; i++) {
            for (int j = 0; j <= tileDimY; j++) {
                sourcePartitions[i][j] = new TIntArrayList();
                targetPartitions[i][j] = new TIntArrayList();
            }
        }

        assignToPartition(sourceData, sourcePartitions);
        assignToPartition(targetData, targetPartitions);
    }

    @Override
    /// Verification method calls ForwardSweepPlane algorithm to report the
    /// geometry pairs.
    public void verification() {
        for (int i = 0; i <= tileDimX; i++) {
            for (int j = 0; j <= tileDimY; j++) {
                verifyPartition(i, j);
            }
        }
        relations.close();
    }

    private void verifyPair(int partitionXId, int partitionYId, int sourceId, int targetId) {
        Geometry source = sourceData[sourceId].getGeometry();
        Geometry target = targetData[targetId].getGeometry();
        if (target.getEnvelopeInternal().intersects(source.getEnvelopeInternal())) {
            //reference point technique
            int xPartition = (int) Math.floor((Math.max(source.getEnvelopeInternal().getMinX(), target.getEnvelopeInternal().getMinX()) - cover.getMinX()) / coverXWidth);
            int yPartition = (int) Math.ceil((Math.min(source.getEnvelopeInternal().getMaxY(), target.getEnvelopeInternal().getMaxY()) - cover.getMinY()) / coverYWidth);
            if (partitionXId == xPartition && partitionYId == yPartition) {
                relations.verifyRelations(sourceId, targetId, source, target);
            }
        }
    }

    private void verifyPartition(int xId, int yId) {
        final SweepStructure sweepStructure = planeSweepStructure == PlaneSweepStructure.LIST_SWEEP
                ? new ListSweep()
                : new StripedSweep();
        sweepStructure.setThetas(sourceData);
        
        final TIntList sourcePartition = sourcePartitions[xId][yId];
        final TIntList targetPartition = targetPartitions[xId][yId];

        int sourceIndex = 0;
        int sourceEndIndex = sourcePartition.size();

        int targetIndex = 0;
        int targetEndIndex = targetPartition.size();
        
        while (sourceIndex < sourceEndIndex) {
            if (targetIndex == targetEndIndex) {
                break;
            }

            GeometryProfile source = sourceData[sourcePartition.get(sourceIndex)];
            GeometryProfile candidate = targetData[targetPartition.get(targetIndex)];
            if (candidate == null || source.getMaxX() < candidate.getMinX()) {
                sweepStructure.removeExpired(source);

                final TIntSet partialCandidates = sweepStructure.reportOverlap(source, sourcePartition.get(sourceIndex));
                final TIntIterator intIterator = partialCandidates.iterator();
                while (intIterator.hasNext()) {
                    int candidateMatchId = intIterator.next();
                    verifyPair(xId, yId, sourcePartition.get(sourceIndex), candidateMatchId);
                }
                sourceIndex++;
            } else {
                sweepStructure.insert(candidate, targetPartition.get(targetIndex));
                targetIndex++;
            }
        }

        while (sourceIndex < sourceEndIndex) {
            GeometryProfile source = sourceData[sourcePartition.get(sourceIndex)];
            sweepStructure.removeExpired(source);
            final TIntSet partialCandidates = sweepStructure.reportOverlap(source, sourcePartition.get(sourceIndex));
            final TIntIterator intIterator = partialCandidates.iterator();
            while (intIterator.hasNext()) {
                int candidateMatchId = intIterator.next();
                verifyPair(xId, yId, sourcePartition.get(sourceIndex), candidateMatchId);
            }
            sourceIndex++;
        }
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
        return "PBSM";
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
