package progressiveAlgorithms;

import com.google.common.collect.MinMaxPriorityQueue;
import datamodel.GeometryProfile;
import datamodel.Pair;
import datamodel.Tile;
import datareader.AbstractReader;
import utilities.WeightingScheme;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.TIntList;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import org.apache.jena.atlas.json.JsonArray;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import utilities.IncPairWeightComparator;

public class ProgressiveGIAnt extends AbstractProgressiveAlgorithm {

    protected int[] flag;
    protected int[] frequency;

    protected double minimumWeight;

    protected MinMaxPriorityQueue topKPairs;

    public ProgressiveGIAnt(int budget, int qPairs, AbstractReader sourceReader,
            AbstractReader targetReader, WeightingScheme wScheme) {

        super(budget, qPairs, sourceReader, targetReader, wScheme);

        minimumWeight = 0;
        flag = new int[datasetDelimiter];
        frequency = new int[datasetDelimiter];
        topKPairs = MinMaxPriorityQueue.orderedBy(new IncPairWeightComparator()).maximumSize(2 * budget).create();
    }

    public ProgressiveGIAnt(int budget, int qPairs, AbstractReader sourceReader,
                            AbstractReader targetReader, WeightingScheme wScheme, String exportPath) {

        super(budget, qPairs, sourceReader, targetReader, wScheme, exportPath);

        minimumWeight = 0;
        flag = new int[datasetDelimiter];
        frequency = new int[datasetDelimiter];
        topKPairs = MinMaxPriorityQueue.orderedBy(new IncPairWeightComparator()).maximumSize(2 * budget).create();
    }

    protected TIntSet getCandidates(int referenceId, Geometry tEntity) {
        final TIntSet candidateMatches = new TIntHashSet();

        final Envelope envelope = tEntity.getEnvelopeInternal();
        int maxX = (int) Math.ceil(envelope.getMaxX() / thetaX);
        int maxY = (int) Math.ceil(envelope.getMaxY() / thetaY);
        int minX = (int) Math.floor(envelope.getMinX() / thetaX);
        int minY = (int) Math.floor(envelope.getMinY() / thetaY);

        for (int latIndex = minX; latIndex <= maxX; latIndex++) {
            for (int longIndex = minY; longIndex <= maxY; longIndex++) {
                final TIntList partialCandidates = spatialIndex.getSquare(latIndex, longIndex);
                if (partialCandidates != null) {
                    final TIntIterator iterator = partialCandidates.iterator();
                    while (iterator.hasNext()) {
                        int currentId = iterator.next();
                        if (flag[currentId] != referenceId) {
                            flag[currentId] = referenceId;
                            frequency[currentId] = 0;
                        }
                        frequency[currentId]++;
                        candidateMatches.add(currentId);
                    }
                }
            }
        }

        return candidateMatches;
    }

    @Override
    public String getMethodName() {
        return "Progressive GIA.nt";
    }

    @Override
    protected float getNoOfCommonBlocks(int sourceId) {
        return frequency[sourceId];
    }

    @Override
    protected void scheduling() {
        int counter = 0;
        while (targetReader.hasNext()) {
            GeometryProfile profile = targetReader.next();
            if (profile == null) {
                continue;
            }
            
            final TIntSet candidateMatches = getCandidates(counter, profile.getGeometry());

            final TIntIterator intIterator = candidateMatches.iterator();
            while (intIterator.hasNext()) {
                int candidateMatchId = intIterator.next();
                if (validCandidate(candidateMatchId, profile.getGeometry().getEnvelopeInternal(), null)) {
                    float weight = getWeight(candidateMatchId, profile.getGeometry().getEnvelopeInternal());
                    if (minimumWeight < weight) {
                        final Pair p = new Pair(candidateMatchId, counter, weight, profile.getGeometry());
                        topKPairs.add(p);
                        if (budget < topKPairs.size()) {
                            Pair lastPair = (Pair) topKPairs.poll();
                            minimumWeight = lastPair.getWeight();
                        }
                    }
                }
            }

            counter++;
        }
    }

    @Override
    protected void setThetas() {
        thetaX = 0;
        thetaY = 0;
        for (GeometryProfile sEntity : sourceData) {
            final Envelope en = sEntity.getGeometry().getEnvelopeInternal();
            thetaX += en.getWidth();
            thetaY += en.getHeight();
        }
        thetaX /= sourceData.length;
        thetaY /= sourceData.length;
        System.out.println(thetaX + "\t" + thetaY);
    }

    @Override
    protected boolean validCandidate(int candidateId, Envelope e2, Tile bTile) {
        return sourceData[candidateId].getGeometry().getEnvelopeInternal().intersects(e2);
    }

    @Override
    protected void verification() {
        while (!topKPairs.isEmpty()) {
            final Pair p = (Pair) topKPairs.pollLast();
            relations.verifyRelations(p.getEntityId1(), p.getEntityId2(), sourceData[p.getEntityId1()].getGeometry(), p.getTargetGeometry());
        }
        relations.close();
    }

    @Override
    public String getMethodConfiguration() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String getMethodInfo() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String getMethodParameters() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public JsonArray getParameterConfiguration() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String getParameterDescription(int parameterId) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String getParameterName(int parameterId) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
