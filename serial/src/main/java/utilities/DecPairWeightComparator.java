package utilities;

import datamodel.Pair;
import java.util.Comparator;

public class DecPairWeightComparator implements Comparator<Pair> {

    /* 
    * This comparator orders comparisons in decreasing order of weight 
    * (utility measure), i.e., from the largest weight to the smallest one.
    * It is useful for Cardinality Node Pruning.
    */
    
    @Override
    public int compare(Pair p1, Pair p2) {
        float test = p1.getWeight()- p2.getWeight(); 
        if (0 < test) {
            return -1;
        }

        if (test < 0) {
            return 1;
        }

        return 0;
    }
    
//    public static void main (String[] args) {
//        Comparison c1 = new Comparison(true, 0, 1);
//        c1.setUtilityMeasure(0.5);
//        Comparison c2 = new Comparison(true, 0, 2);
//        c2.setUtilityMeasure(0.25);
//        Comparison c3 = new Comparison(true, 0, 3);
//        c3.setUtilityMeasure(0.75);
//        
//        List<Comparison> comparisons = new ArrayList<>();
//        comparisons.add(c1);
//        comparisons.add(c2);
//        comparisons.add(c3);
//        
//        Collections.sort(comparisons, new DecComparisonWeightComparator());
//        
//        comparisons.forEach((c) -> {
//            System.out.println(c.toString());
//        });
//    }
}