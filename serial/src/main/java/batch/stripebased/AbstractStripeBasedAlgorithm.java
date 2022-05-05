package batch.stripebased;

import batch.AbstractBatchAlgorithm;
import datamodel.*;
import datareader.AbstractReader;
import org.locationtech.jts.geom.Envelope;

public abstract class AbstractStripeBasedAlgorithm extends AbstractBatchAlgorithm {

    protected double thetaX;

    protected StripeIndex spatialIndex;

    public AbstractStripeBasedAlgorithm(int qPairs, AbstractReader sourceReader, AbstractReader targetReader) {
        super(qPairs, sourceReader, targetReader);
    }

    public AbstractStripeBasedAlgorithm(int qPairs, AbstractReader sourceReader, AbstractReader targetReader, String exportPath) {
        super(qPairs, sourceReader, targetReader, exportPath);
    }

    @Override
    protected void filtering() {
        setThetas();
        indexSource();
    }

    private void indexSource() {
        spatialIndex = new StripeIndex();
        for (int i = 0; i < datasetDelimiter; i++) {
            addToIndex(i, sourceData[i].getGeometry().getEnvelopeInternal(), spatialIndex);
        }
    }

    /**
     * current thetaX is computed by the number of stripes user gives as input a
     * secondary implementation that thetaX is set as the average is needed
     * (similar to giant)
     */
    protected void setThetas() {
        thetaX = 0;
        for (GeometryProfile profile : sourceData) {
            final Envelope en = profile.getGeometry().getEnvelopeInternal();
            thetaX += en.getWidth();
        }
        thetaX /= sourceData.length;
        System.out.println(thetaX);
    }

    protected abstract void addToIndex(int geometryId, Envelope envelope, StripeIndex stripeIndex);
}
