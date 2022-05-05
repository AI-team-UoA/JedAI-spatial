package batch;

import datamodel.GeometryProfile;
import datamodel.RelatedGeometries;
import datareader.AbstractReader;
import utilities.IDocumentation;

public abstract class AbstractBatchAlgorithm implements IDocumentation {
    
    protected final int datasetDelimiter;

    protected long indexingTime;
    protected long verificationTime;

    protected final GeometryProfile[] sourceData;
    protected final RelatedGeometries relations;
    protected final AbstractReader targetReader;
    
    public AbstractBatchAlgorithm(int qPairs, AbstractReader sourceReader, AbstractReader targetReader) {
        sourceData = sourceReader.getGeometryProfiles();
        
        relations = new RelatedGeometries(qPairs);
        datasetDelimiter = sourceData.length;
        
        this.targetReader = targetReader;
    }

    public AbstractBatchAlgorithm(int qPairs, AbstractReader sourceReader, AbstractReader targetReader, String exportPath) {
        sourceData = sourceReader.getGeometryProfiles();

        relations = new RelatedGeometries(qPairs, exportPath);
        datasetDelimiter = sourceData.length;

        this.targetReader = targetReader;
    }
    
    public void applyProcessing() {
        long time1 = System.currentTimeMillis();
        filtering();
        long time2 = System.currentTimeMillis();
        verification();
        long time3 = System.currentTimeMillis();
        indexingTime = time2 - time1;
        verificationTime = time3 - time2;
    }
    
    protected abstract void filtering();
        
    public void printResults() {
        System.out.println(getResultsText());
    }
    
    public long getIndexingTime() {
        return indexingTime;
    }
    
    public RelatedGeometries getResults() {
        return relations;
    }
    
    public String getResultsText() {
        StringBuilder sb = new StringBuilder();
        sb.append("Source geometries\t:\t").append(sourceData.length).append("\n");
        sb.append("Target geometries\t:\t").append(targetReader.getSize()).append("\n");
        sb.append("Indexing time\t:\t").append(indexingTime).append("\n");
        sb.append("Verification time\t:\t").append(verificationTime).append("\n");
        sb.append(relations.print()).append("\n");
        return sb.toString();
    }
    
    public int getSourceGeometries() {
        return sourceData.length;
    }
    
    public int getTargetGeometries() {
        return targetReader.getSize();
    }
    
    public long getVerificationTime() {
        return verificationTime;
    }
    
    protected abstract void verification();
}
