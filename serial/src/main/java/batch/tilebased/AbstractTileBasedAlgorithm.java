package batch.tilebased;

import batch.AbstractBatchAlgorithm;
import datamodel.LightIndex;
import datareader.AbstractReader;
import org.locationtech.jts.geom.Envelope;

public abstract class AbstractTileBasedAlgorithm extends AbstractBatchAlgorithm {

    protected double thetaX;
    protected double thetaY;

    protected LightIndex spatialIndex;

    public AbstractTileBasedAlgorithm(int qPairs, AbstractReader sourceReader, AbstractReader targetReader) {
        super(qPairs, sourceReader, targetReader);
    }

    public AbstractTileBasedAlgorithm(int qPairs, AbstractReader sourceReader, AbstractReader targetReader, String exportPath) {
        super(qPairs, sourceReader, targetReader, exportPath);
    }

    protected void addToIndex(int geometryId, Envelope envelope, LightIndex spatialIndex) {
        int maxX = (int) Math.ceil(envelope.getMaxX() / thetaX);
        int maxY = (int) Math.ceil(envelope.getMaxY() / thetaY);
        int minX = (int) Math.floor(envelope.getMinX() / thetaX);
        int minY = (int) Math.floor(envelope.getMinY() / thetaY);
        for (int latIndex = minX; latIndex <= maxX; latIndex++) {
            for (int longIndex = minY; longIndex <= maxY; longIndex++) {
                spatialIndex.add(latIndex, longIndex, geometryId);
            }
        }
    }

    @Override
    protected void filtering() {
        setThetas();
        indexSource();
    }

    private void indexSource() {
        spatialIndex = new LightIndex();
        for (int i = 0; i < datasetDelimiter; i++) {
            addToIndex(i, sourceData[i].getGeometry().getEnvelopeInternal(), spatialIndex);
        }
    }

    protected abstract void setThetas();
}
