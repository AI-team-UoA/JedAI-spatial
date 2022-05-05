package progressiveAlgorithms;

import datamodel.*;
import datareader.AbstractReader;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TIntObjectMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.locationtech.jts.geom.Envelope;
import utilities.DecPairWeightComparator;
import utilities.DecTileCardinalityComparator;
import utilities.IncTileCardinalityComparator;
import utilities.WeightingScheme;
import org.apache.jena.atlas.json.JsonArray;

public class ProgressiveRADON extends AbstractProgressiveAlgorithm {

    protected final boolean decreasingOrder;

    protected List<Tile> blocksList;
    protected GeometryIndex geometryIndex;
    protected final GeometryProfile[] targetData;

    public ProgressiveRADON(boolean decOrder, int budget, int qPairs, AbstractReader sourceReader,
            AbstractReader targetReader, WeightingScheme wScheme) {
        super(budget, qPairs, sourceReader, targetReader, wScheme);

        decreasingOrder = decOrder;
        targetData = targetReader.getGeometryProfiles();
    }

    public ProgressiveRADON(boolean decOrder, int budget, int qPairs, AbstractReader sourceReader,
                            AbstractReader targetReader, WeightingScheme wScheme, String exportPath) {
        super(budget, qPairs, sourceReader, targetReader, wScheme, exportPath);

        decreasingOrder = decOrder;
        targetData = targetReader.getGeometryProfiles();
    }

    private List<Tile> getBlocks(LightIndex spatialIndex) {
        final List<Tile> blocks = new ArrayList<>();
        final TIntObjectMap<TIntObjectMap<TIntList>> map = spatialIndex.getMap();
        final TIntObjectIterator<TIntObjectMap<TIntList>> outerIterator = map.iterator();
        while (outerIterator.hasNext()) {
            outerIterator.advance();

            final int xDimension = outerIterator.key();
            final TIntObjectMap<TIntList> value = outerIterator.value();
            final TIntObjectIterator<TIntList> innerIterator = value.iterator();
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
        return "Progressive RADON";
    }

    protected float getNoOfBlocks() {
        return geometryIndex.getNoOfBlocks();
    }

    protected float getNoOfCommonBlocks(int sourceId, int targetId) {
        return geometryIndex.getNoOfCommonBlocks(sourceId, targetId);
    }

    protected float getNoOfGeometryBlocks(int geometryId) {
        return geometryIndex.getNoOfEntityBlocks(geometryId);
    }

    protected float getWeight(int sourceId, int targetId) {
        float commonBlocks = getNoOfCommonBlocks(sourceId, targetId);
        switch (wScheme) {
            case CF:
                return commonBlocks;
            case JS:
                return commonBlocks / (getNoOfGeometryBlocks(sourceId) + getNoOfGeometryBlocks(targetId) - commonBlocks);
            case X2:
                long[] v = new long[2];
                v[0] = (long) commonBlocks;
                v[1] = (long) getNoOfGeometryBlocks(sourceId) - v[0];

                long[] v_ = new long[2];
                v_[0] = (long) getNoOfGeometryBlocks(targetId) - v[0];
                v_[1] = (long) (getNoOfBlocks() - (v[0] + v[1] + v_[0]));

                return (float) chiSquaredTest.chiSquare(new long[][]{v, v_});
        }

        return 1.0f;
    }

    @Override
    protected void filtering() {
        super.filtering();
        indexTarget();
    }

    private void indexTarget() {
        int geometryId = datasetDelimiter;
        for (GeometryProfile tEntity : targetData) {
            final Envelope envelope = tEntity.getGeometry().getEnvelopeInternal();
            addToIndex(geometryId++, envelope, spatialIndex);
        }
    }

    @Override
    public void scheduling() {
        blocksList = getBlocks(spatialIndex);
        if (decreasingOrder) {
            Collections.sort(blocksList, new DecTileCardinalityComparator());
        } else {
            Collections.sort(blocksList, new IncTileCardinalityComparator());
        }
        geometryIndex = new GeometryIndex(datasetDelimiter, sourceData.length + targetData.length, blocksList.toArray(new Tile[blocksList.size()]));
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

    @Override
    protected void verification() {
        int verifiedPairs = -1;
        final List<Pair> topPairs = new ArrayList<>();
        for (Tile block : blocksList) {
            topPairs.clear();
            final PairIterator iterator = block.getPairIterator();
            while (iterator.hasNext()) {
                final Pair pair = iterator.next();
                if (validCandidate(pair.getEntityId1(), targetData[pair.getEntityId2()].getGeometry().getEnvelopeInternal(), (Tile) block)) {
                    float weight = getWeight(pair.getEntityId1(), pair.getEntityId2() + datasetDelimiter);
                    pair.setWeight(weight);
                    topPairs.add(pair);
                }
            }
            Collections.sort(topPairs, new DecPairWeightComparator());

            for (Pair p : topPairs) {
                verifiedPairs++;
                if (verifiedPairs == budget) {
                    return;
                }
                relations.verifyRelations(p.getEntityId1(), p.getEntityId2(), sourceData[p.getEntityId1()].getGeometry(), targetData[p.getEntityId2()].getGeometry());
                if (relations.getVerifiedPairs() % 631064 == 0) {
                    System.out.println(relations.getVerifiedPairs() + "\t" + relations.getInterlinkedPairs());
                }
            }
        }
        relations.close();
    }

    @Override
    protected boolean validCandidate(int candidateId, Envelope e2, Tile bTile) {
        int xDimension = (int) Math.max(sourceData[candidateId].getGeometry().getEnvelopeInternal().getMinX() / thetaX, e2.getMinX() / thetaX);
        int yDimension = (int) Math.min(sourceData[candidateId].getGeometry().getEnvelopeInternal().getMaxY() / thetaY, e2.getMaxY() / thetaY);
        if (xDimension == bTile.getXDimension() && yDimension == bTile.getYDimension()) {
            return sourceData[candidateId].getGeometry().getEnvelopeInternal().intersects(e2);
        }
        return false;
    }

    @Override
    protected float getNoOfCommonBlocks(int sourceId) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
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
