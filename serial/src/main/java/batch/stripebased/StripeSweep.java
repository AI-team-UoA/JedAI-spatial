package batch.stripebased;

import datamodel.GeometryProfile;
import datamodel.StripeIndex;
import datareader.AbstractReader;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.TIntList;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import org.apache.jena.atlas.json.JsonArray;
import org.locationtech.jts.geom.Envelope;

/**
 * Plane sweep algorithm with multiple stripes
 */
public class StripeSweep extends AbstractStripeBasedAlgorithm {

    public StripeSweep(int qPairs, AbstractReader sourceReader, AbstractReader targetReader) {
        super(qPairs, sourceReader, targetReader);
    }

    public StripeSweep(int qPairs, AbstractReader sourceReader, AbstractReader targetReader, String exportFile) {
        super(qPairs, sourceReader, targetReader, exportFile);
    }

    @Override
    protected void verification() {
        int counter = 0;
        while (targetReader.hasNext()) {
            counter++;
            GeometryProfile profile = targetReader.next();
            if (profile == null) {
                continue;
            }

            final TIntSet candidateMatches = new TIntHashSet();
            final Envelope envelope = profile.getGeometry().getEnvelopeInternal();

            int maxX = (int) Math.ceil(envelope.getMaxX() / thetaX);
            int minX = (int) Math.floor(envelope.getMinX() / thetaX);
            for (int longIndex = minX; longIndex <= maxX; longIndex++) {
                final TIntList partialCandidates = spatialIndex.getStripe(longIndex);
                if (partialCandidates != null) {
                    candidateMatches.addAll(partialCandidates);
                }
            }

            final TIntIterator intIterator = candidateMatches.iterator();
            while (intIterator.hasNext()) {
                int candidateMatchId = intIterator.next();
                if (sourceData[candidateMatchId].getGeometry().getEnvelopeInternal().intersects(envelope)) {
                    relations.verifyRelations(candidateMatchId, counter, sourceData[candidateMatchId].getGeometry(), profile.getGeometry());
                }
            }
        }
        targetReader.close();
        relations.close();
    }

    @Override
    protected void addToIndex(int geometryId, Envelope envelope, StripeIndex spatialIndex) {
        int maxX = (int) Math.ceil(envelope.getMaxX() / thetaX);
        int minX = (int) Math.floor(envelope.getMinX() / thetaX);
        for (int longIndex = minX; longIndex <= maxX; longIndex++) {
            spatialIndex.add(longIndex, geometryId);
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
        return "StripeSweep";
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
