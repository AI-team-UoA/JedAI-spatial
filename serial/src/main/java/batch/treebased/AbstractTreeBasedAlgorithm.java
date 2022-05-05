package batch.treebased;

import batch.AbstractBatchAlgorithm;
import datamodel.GeometryProfile;
import datareader.AbstractReader;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.TIntList;
import org.locationtech.jts.geom.Envelope;

public abstract class AbstractTreeBasedAlgorithm extends AbstractBatchAlgorithm {

    public AbstractTreeBasedAlgorithm(int qPairs, AbstractReader sourceReader, AbstractReader targetReader) {
        super(qPairs, sourceReader, targetReader);
    }

    public AbstractTreeBasedAlgorithm(int qPairs, AbstractReader sourceReader, AbstractReader targetReader, String exportPath) {
        super(qPairs, sourceReader, targetReader, exportPath);
    }

    @Override
    protected void filtering() {
        indexSource();
    }

    protected abstract TIntList getCandidates(Envelope envelope);

    protected abstract void indexSource();

    @Override
    protected void verification() {
        int counter = 0;
        while (targetReader.hasNext()) {
            counter++;
            GeometryProfile profile = targetReader.next();
            if (profile == null) {
                continue;
            }

            final Envelope envelope = profile.getGeometry().getEnvelopeInternal();
            final TIntList partialCandidates = getCandidates(envelope);
            if (partialCandidates == null) {
                continue;
            }

            final TIntIterator intIterator = partialCandidates.iterator();
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
}
