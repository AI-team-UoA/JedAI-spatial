package batch.treebased;

import datareader.AbstractReader;
import gnu.trove.list.TIntList;
import org.apache.jena.atlas.json.JsonArray;
import org.locationtech.jts.geom.Envelope;
import datamodel.rtree.RTreeIndex;

/**
 * An implementation of Guttman's R-Tree as a Spatial Index
 */
public class RTree extends AbstractTreeBasedAlgorithm {

    private final RTreeIndex spatialIndex;

    public RTree(int qPairs, AbstractReader sourceReader, AbstractReader targetReader) {
        super(qPairs, sourceReader, targetReader);
        this.spatialIndex = new RTreeIndex();
    }

    public RTree(int qPairs, AbstractReader sourceReader, AbstractReader targetReader, String exportPath) {
        super(qPairs, sourceReader, targetReader, exportPath);
        this.spatialIndex = new RTreeIndex();
    }

    @Override
    protected TIntList getCandidates(Envelope envelope) {
        return spatialIndex.rangeSearch(envelope);
    }

    public String getMethodConfiguration() {
        return null;
    }

    public String getMethodInfo() {
        return null;
    }

    public String getMethodName() {
        return "RTree";
    }

    public String getMethodParameters() {
        return null;
    }

    public JsonArray getParameterConfiguration() {
        return null;
    }

    public String getParameterDescription(int parameterId) {
        return null;
    }

    public String getParameterName(int parameterId) {
        return null;
    }
    
    @Override
    protected void indexSource() {
        for (int i = 0; i < datasetDelimiter; i++) {
            spatialIndex.insert(sourceData[i], i);
        }
    }
}
