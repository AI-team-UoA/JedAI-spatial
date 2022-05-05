package batch.tilebased;

import datamodel.*;
import datareader.AbstractReader;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TIntObjectMap;
import java.util.ArrayList;
import java.util.List;
import org.apache.jena.atlas.json.JsonArray;
import org.locationtech.jts.geom.Envelope;

public class RADON extends AbstractTileBasedAlgorithm {

    protected List<Tile> blocksList;
    protected final GeometryProfile[] targetData;

    public RADON(int qPairs, AbstractReader sourceReader, AbstractReader targetReader) {
        super(qPairs, sourceReader, targetReader);
        targetData = targetReader.getGeometryProfiles();
    }

    public RADON(int qPairs, AbstractReader sourceReader, AbstractReader targetReader, String  exportFile) {
        super(qPairs, sourceReader, targetReader);
        targetData = targetReader.getGeometryProfiles();
    }
    
    @Override
    protected void filtering() {
        super.filtering();
        indexTarget();
    }

    private List<Tile> getBlocks(LightIndex spatialIndex) {
        final List<Tile> blocks = new ArrayList<>();
        final TIntObjectIterator<TIntObjectMap<TIntList>> outerIterator = spatialIndex.getMap().iterator();
        while (outerIterator.hasNext()) {
            outerIterator.advance();

            final int xDimension = outerIterator.key();
            final TIntObjectIterator<TIntList> innerIterator = outerIterator.value().iterator();
            while (innerIterator.hasNext()) {
                innerIterator.advance();

                if (innerIterator.value().size() < 2) {
                    continue;
                }

                final int yDimension = innerIterator.key();
                final TIntList entitiesD1 = new TIntArrayList();
                final TIntList entitiesD2 = new TIntArrayList();
                final TIntIterator entityIterator = innerIterator.value().iterator();
                while (entityIterator.hasNext()) {
                    int currentId = entityIterator.next();
                    if (currentId < datasetDelimiter) {
                        entitiesD1.add(currentId);
                    } else {
                        entitiesD2.add(currentId - datasetDelimiter);
                    }
                }

                // each block should contain entities from both dataset
                if (entitiesD1.isEmpty() || entitiesD2.isEmpty()) {
                    continue;
                }

                blocks.add(new Tile(xDimension, yDimension, entitiesD1.toArray(), entitiesD2.toArray()));
            }
        }

        return blocks;
    }

    @Override
    public String getMethodName() {
        return "Optimized RADON";
    }

    private void indexTarget() {
        int geometryId = datasetDelimiter;
        for (GeometryProfile tEntity : targetData) {
            addToIndex(geometryId++, tEntity.getGeometry().getEnvelopeInternal(), spatialIndex);
        }
    }

    @Override
    protected void setThetas() {
        float thetaXa = 0;
        float thetaYa = 0;
        for (GeometryProfile sEntity : sourceData) {
            final Envelope en = sEntity.getGeometry().getEnvelopeInternal();
            thetaXa += en.getWidth();
            thetaYa += en.getHeight();
        }
        thetaXa /= sourceData.length;
        thetaYa /= sourceData.length;

        float thetaXb = 0;
        float thetaYb = 0;
        for (GeometryProfile tEntity : targetData) {
            final Envelope en = tEntity.getGeometry().getEnvelopeInternal();
            thetaXb += en.getWidth();
            thetaYb += en.getHeight();
        }
        thetaXb /= targetData.length;
        thetaYb /= targetData.length;
        thetaX = 0.5f * (thetaXa + thetaXb);
        thetaY = 0.5f * (thetaYa + thetaYb);
        System.out.println(thetaX + "\t" + thetaY);
    }

    // reference point technique to remove duplicate pairs
    protected boolean validCandidate(int sourceId, int targetId, Tile bTile) {
        final Envelope sourceEnv = sourceData[sourceId].getGeometry().getEnvelopeInternal();
        final Envelope targetEnv = targetData[targetId].getGeometry().getEnvelopeInternal();
        int xDimension = (int) Math.max(sourceEnv.getMinX() / thetaX, targetEnv.getMinX() / thetaX);
        int yDimension = (int) Math.min(sourceEnv.getMaxY() / thetaY, targetEnv.getMaxY() / thetaY);
        if (xDimension == bTile.getXDimension() && yDimension == bTile.getYDimension()) {
            return sourceEnv.intersects(targetEnv);
        }
        return false;
    }
    
    @Override
    protected void verification() {
        blocksList = getBlocks(spatialIndex);
        for (Tile block : blocksList) {
            final PairIterator iterator = block.getPairIterator();
            while (iterator.hasNext()) {
                final Pair pair = iterator.next();
                if (validCandidate(pair.getEntityId1(), pair.getEntityId2(), block)) {
                   relations.verifyRelations(pair.getEntityId1(), pair.getEntityId2(), 
                           sourceData[pair.getEntityId1()].getGeometry(), targetData[pair.getEntityId2()].getGeometry()); 
                }
            }
        }
        relations.close();
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
}
