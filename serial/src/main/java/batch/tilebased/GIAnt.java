package batch.tilebased;

import datamodel.GeometryProfile;
import datareader.AbstractReader;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.TIntList;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import org.apache.jena.atlas.json.JsonArray;
import org.locationtech.jts.geom.Envelope;

public class GIAnt extends AbstractTileBasedAlgorithm {

    public GIAnt(int qPairs, AbstractReader sourceReader, AbstractReader targetReader) {
        super(qPairs, sourceReader, targetReader);
    }

    public GIAnt(int qPairs, AbstractReader sourceReader, AbstractReader targetReader, String exportPath) {
        super(qPairs, sourceReader, targetReader, exportPath);
    }

    @Override
    public String getMethodConfiguration() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String getMethodInfo() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String getMethodName() {
        return "GIA.nt";
    }

    @Override
    public String getMethodParameters() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public JsonArray getParameterConfiguration() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String getParameterDescription(int parameterId) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String getParameterName(int parameterId) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    @Override
    protected void setThetas() {
        thetaX = 0;
        thetaY = 0;
        for (GeometryProfile profile : sourceData) {
            final Envelope en = profile.getGeometry().getEnvelopeInternal();
            thetaX += en.getWidth();
            thetaY += en.getHeight();
        }
        thetaX /= sourceData.length;
        thetaY /= sourceData.length;
        System.out.println(thetaX + "\t" + thetaY);
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
                       
//            if (profile.getGeometry().getGeometryType().equals("GeometryCollection")) {
//                Log.warn("Entity collection was read and will be skipped.");
//                continue;
//            }
            
            final TIntSet candidateMatches = new TIntHashSet();
            final Envelope envelope = profile.getGeometry().getEnvelopeInternal();

            int maxX = (int) Math.ceil(envelope.getMaxX() / thetaX);
            int maxY = (int) Math.ceil(envelope.getMaxY() / thetaY);
            int minX = (int) Math.floor(envelope.getMinX() / thetaX);
            int minY = (int) Math.floor(envelope.getMinY() / thetaY);
            for (int latIndex = minX; latIndex <= maxX; latIndex++) {
                for (int longIndex = minY; longIndex <= maxY; longIndex++) {
                    final TIntList partialCandidates = spatialIndex.getSquare(latIndex, longIndex);
                    if (partialCandidates != null) {
                        candidateMatches.addAll(partialCandidates);
                    }
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
}
