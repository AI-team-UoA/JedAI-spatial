package datamodel;

public class Tile {

    private final int xDimension;
    private final int yDimension;

    private final int[] sourceGeometries;
    private final int[] targetGeometries;

    public Tile(int x, int y, int[] entities1, int[] entities2) {
        xDimension = x;
        yDimension = y;
        sourceGeometries = entities1;
        targetGeometries = entities2;
    }

    public float getNoOfPairs() {
        return ((float) sourceGeometries.length) * ((float) targetGeometries.length);
    }

    public PairIterator getPairIterator() {
        return new PairIterator(this);
    }

    public int[] getSourceGeometries() {
        return sourceGeometries;
    }

    public int[] getTargetGeometries() {
        return targetGeometries;
    }

    public int getXDimension() {
        return xDimension;
    }

    public int getYDimension() {
        return yDimension;
    }
}
