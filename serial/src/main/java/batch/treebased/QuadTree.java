package batch.treebased;

import datareader.AbstractReader;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import org.apache.jena.atlas.json.JsonArray;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.index.quadtree.Quadtree;

import java.util.List;

/**
 * Simple Implementation using JTS's QuadTree as a Spatial Index
 */
public class QuadTree extends AbstractTreeBasedAlgorithm {

    private final Quadtree spatialIndex;

    public QuadTree(int qPairs, AbstractReader sourceReader, AbstractReader targetReader) {
        super(qPairs, sourceReader, targetReader);
        this.spatialIndex = new Quadtree();
    }

    public QuadTree(int qPairs, AbstractReader sourceReader, AbstractReader targetReader, String exportPath) {
        super(qPairs, sourceReader, targetReader, exportPath);
        this.spatialIndex = new Quadtree();
    }

    @Override
    protected TIntList getCandidates(Envelope envelope) {
        final List<Integer> ids = spatialIndex.query(envelope);
        final TIntList listIds = new TIntArrayList();
        listIds.addAll(ids);
        return listIds;
    }

    public String getMethodConfiguration() {
        return null;
    }

    public String getMethodInfo() {
        return null;
    }

    public String getMethodName() {
        return "QuadTree";
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
            spatialIndex.insert(sourceData[i].getGeometry().getEnvelopeInternal(), i);
        }
    }
}
