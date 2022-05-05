package utilities;

import datamodel.Tile;
import java.util.Comparator;

public class IncTileCardinalityComparator implements Comparator<Tile> {

    /* 
    * This comparator orders blocks in increasing order of cardinality, i.e.,
    * from the smallest number of comparisons to the largest one.
    * It is useful for Block Cleaning techniques.
    * Blocks with the same number of comparisons are ordered in decreasing aggregate comparison weight.
     */
    @Override
    public int compare(Tile block1, Tile block2) {
        return Double.compare(block1.getNoOfPairs(), block2.getNoOfPairs());
    }
}
