package batch.stripebased;

import datamodel.GeometryProfile;
import datamodel.StripeSTRTreeIndex;
import datamodel.StripeIndex;
import datareader.AbstractReader;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import org.apache.jena.atlas.json.JsonArray;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.index.strtree.STRtree;

public class StripeSTRSweep extends AbstractStripeBasedAlgorithm {

    protected StripeSTRTreeIndex stripeSTRTreeIndex;

    public StripeSTRSweep(int qPairs, AbstractReader sourceReader, AbstractReader targetReader) {
        super(qPairs, sourceReader, targetReader);
        this.stripeSTRTreeIndex = new StripeSTRTreeIndex();
    }

    public StripeSTRSweep(int qPairs, AbstractReader sourceReader, AbstractReader targetReader, String exportPath) {
        super(qPairs, sourceReader, targetReader, exportPath);
        this.stripeSTRTreeIndex = new StripeSTRTreeIndex();
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
                // instead of adding all search in str tree which ones match
                STRtree stRtree = stripeSTRTreeIndex.getSTRTree(longIndex);
                if (stRtree != null) {
                    candidateMatches.addAll(stRtree.query(envelope));
                } 
            }

            final TIntIterator intIterator = candidateMatches.iterator();
            while (intIterator.hasNext()) {
                int candidateMatchId = intIterator.next();
                relations.verifyRelations(candidateMatchId, counter, sourceData[candidateMatchId].getGeometry(), profile.getGeometry());
            }
        }
        targetReader.close();
        relations.close();
    }

    @Override
    protected void addToIndex(int geometryId, Envelope envelope, StripeIndex stripeIndex) {
        int maxX = (int) Math.ceil(envelope.getMaxX() / thetaX);
        int minX = (int) Math.floor(envelope.getMinX() / thetaX);
        for (int longIndex = minX; longIndex <= maxX; longIndex++) {
            stripeSTRTreeIndex.add(longIndex, envelope, geometryId);
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
        return "StripeSTRSweep";
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
