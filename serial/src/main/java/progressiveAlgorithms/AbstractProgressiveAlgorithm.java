package progressiveAlgorithms;

import datareader.AbstractReader;
import datamodel.GeometryProfile;
import datamodel.LightIndex;
import datamodel.RelatedGeometries;
import datamodel.Tile;
import utilities.WeightingScheme;
import org.apache.commons.math3.stat.inference.ChiSquareTest;
import org.locationtech.jts.geom.Envelope;
import utilities.IDocumentation;

public abstract class AbstractProgressiveAlgorithm implements IDocumentation {

    protected long indexingTime;
    protected long initializationTime;
    protected long verificationTime;

    protected float thetaX;
    protected float thetaY;

    protected int budget;
    protected int datasetDelimiter;
    protected int noOfApproxBlocks;

    protected final AbstractReader targetReader;
    protected ChiSquareTest chiSquaredTest;
    protected final GeometryProfile[] sourceData;
    protected LightIndex spatialIndex;
    protected final RelatedGeometries relations;
    protected final WeightingScheme wScheme;

    AbstractProgressiveAlgorithm(int budget, int qPairs, AbstractReader sourceReader,
                                 AbstractReader targetReader, WeightingScheme wScheme) {
        this.budget = budget;
        this.wScheme = wScheme;

        sourceData = sourceReader.getGeometryProfiles();
        this.targetReader = targetReader;

        datasetDelimiter = sourceData.length;
        relations = new RelatedGeometries(qPairs);
        if (wScheme.equals(WeightingScheme.X2)) {
            chiSquaredTest = new ChiSquareTest();
        }
    }

    AbstractProgressiveAlgorithm(int budget, int qPairs, AbstractReader sourceReader, 
            AbstractReader targetReader, WeightingScheme wScheme, String exportPath) {
        this.budget = budget;
        this.wScheme = wScheme;

        sourceData = sourceReader.getGeometryProfiles();
        this.targetReader = targetReader;
        
        datasetDelimiter = sourceData.length;
        relations = new RelatedGeometries(qPairs, exportPath);
        if (wScheme.equals(WeightingScheme.X2)) {
            chiSquaredTest = new ChiSquareTest();
        }
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

    public void applyProcessing() {
        long time1 = System.currentTimeMillis();
        filtering();
        long time2 = System.currentTimeMillis();
        scheduling();
        long time3 = System.currentTimeMillis();
        verification();
        long time4 = System.currentTimeMillis();
        indexingTime = time2 - time1;
        initializationTime = time3 - time2;
        verificationTime = time4 - time3;
    }

    protected void filtering() {
        setThetas();
        indexSource();
    }

    protected long getNoOfBlocks(Envelope envelope) {
        int maxX = (int) Math.ceil(envelope.getMaxX() / thetaX);
        int maxY = (int) Math.ceil(envelope.getMaxY() / thetaY);
        int minX = (int) Math.floor(envelope.getMinX() / thetaX);
        int minY = (int) Math.floor(envelope.getMinY() / thetaY);
        return (maxX - minX + 1) * (maxY - minY + 1);
    }

    protected float getWeight(int sourceId, Envelope targetEnv) {
        float commonBlocks = getNoOfCommonBlocks(sourceId);
        switch (wScheme) {
            case CF:
                return commonBlocks;
            case JS:
                final Envelope sourceEnv = sourceData[sourceId].getGeometry().getEnvelopeInternal();
                return commonBlocks / (getNoOfBlocks(sourceEnv) + getNoOfBlocks(targetEnv) - commonBlocks);
            case X2:
                long[] va  = new long[2];
                va[0] = (long) commonBlocks;
                va[1] = getNoOfBlocks(targetEnv) - va[0];

                long[] va_ = new long[2];
                va_[0] = getNoOfBlocks(sourceData[sourceId].getGeometry().getEnvelopeInternal()) - va[0];
                va_[1] = (int) Math.max(1, noOfApproxBlocks - (va[0] + va[1] + va_[0]));

                return (float) chiSquaredTest.chiSquare(new long[][]{va, va_});
        }

        return 1.0f;
    }

    protected void indexSource() {
        spatialIndex = new LightIndex();
        for (int i = 0; i < datasetDelimiter; i++) {
            addToIndex(i, sourceData[i].getGeometry().getEnvelopeInternal(), spatialIndex);
        }

        if (wScheme.equals(WeightingScheme.X2)) {
            setApproximateNoOfBlocks();
        }
    }

    public void printResults() {
        System.out.println("\n\nCurrent method\t:\t" + getMethodName());
        System.out.println("Indexing Time\t:\t" + indexingTime);
        System.out.println("Initialization Time\t:\t" + initializationTime);
        System.out.println("Verification Time\t:\t" + verificationTime);
        System.out.println(relations.print());
    }
    
    protected void setApproximateNoOfBlocks() {
        int globalMaxX = 0;
        int globalMaxY = 0;
        int globalMinX = 0;
        int globalMinY = 0;
        for (GeometryProfile sEntity : sourceData) {
            final Envelope envelope = sEntity.getGeometry().getEnvelopeInternal();
            int maxX = (int) Math.ceil(envelope.getMaxX() / thetaX);
            int maxY = (int) Math.ceil(envelope.getMaxY() / thetaY);
            int minX = (int) Math.floor(envelope.getMinX() / thetaX);
            int minY = (int) Math.floor(envelope.getMinY() / thetaY);

            if (globalMaxX < maxX) {
                globalMaxX = maxX;
            }

            if (globalMaxY < maxY) {
                globalMaxY = maxY;
            }

            if (minX < globalMinX) {
                globalMinX = minX;
            }

            if (minY < globalMinY) {
                globalMinY = minY;
            }
        }

        noOfApproxBlocks = (globalMaxX - globalMinX + 1) * (globalMaxY - globalMinY + 1);
    }

    protected abstract float getNoOfCommonBlocks(int sourceId);
    
    protected abstract void scheduling();
    
    protected abstract void setThetas();

    protected abstract boolean validCandidate(int candidateId, Envelope e2, Tile bTile);

    protected abstract void verification();
}
