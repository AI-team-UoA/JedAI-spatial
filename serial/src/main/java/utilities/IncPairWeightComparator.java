package utilities;

import datamodel.Pair;
import java.util.Comparator;

public class IncPairWeightComparator implements Comparator<Pair> {
    
   /* 
    * This comparator orders pairs in increasing order of weight, i.e., from 
    * the smallest weight to the largest one.
    * It is useful for PriorityQueues of fixed size, so that the lowest weighted 
    * comparison is placed at the head of the queue so that it is quickly 
    * removed with poll().
    */
    
    @Override
    public int compare(Pair o1, Pair o2) {
        float test = o2.getWeight()-o1.getWeight(); 
        if (0 < test) {
            return -1;
        }

        if (test < 0) {
            return 1;
        }

        return 0;
    }
    
//    public static void main(String[] args) {
//        final MinMaxPriorityQueue<Pair> topPairs = MinMaxPriorityQueue.orderedBy(new IncPairWeightComparator()).maximumSize(20).create();
//        topPairs.add(new Pair(0, 1, 0.2f, null));
//        topPairs.add(new Pair(0, 1, 0.1f, null));
//        topPairs.add(new Pair(0, 1, 0.3f, null));
//        topPairs.add(new Pair(0, 1, 0.2f, null));
//        topPairs.add(new Pair(0, 1, 0.2f, null));
//
//        while (!topPairs.isEmpty()) {
////            final Pair firstPair = topPairs.pollLast();
//            final Pair firstPair = topPairs.poll();
//            System.out.println(firstPair.toString());
//        }
//    }
}