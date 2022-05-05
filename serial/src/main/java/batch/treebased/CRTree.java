package batch.treebased;

import datareader.AbstractReader;
import gnu.trove.list.TIntList;
import org.apache.jena.atlas.json.JsonArray;
import org.locationtech.jts.geom.Envelope;
import datamodel.crtree.CRTreeIndex;

/**
 * An implementation of Cache-Conscious R-Tree as a Spatial Index
 */
public class CRTree extends AbstractTreeBasedAlgorithm {

    private final CRTreeIndex spatialIndex;

    public CRTree(int qPairs, AbstractReader sourceReader, AbstractReader targetReader) {
        super(qPairs, sourceReader, targetReader);

        this.spatialIndex = new CRTreeIndex();
    }

    public CRTree(int qPairs, AbstractReader sourceReader, AbstractReader targetReader, String exportPath) {
        super(qPairs, sourceReader, targetReader, exportPath);

        this.spatialIndex = new CRTreeIndex();
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
        return "CRTree";
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
        spatialIndex.bulkLoad(sourceData);
    }
}
