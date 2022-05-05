package progressiveAlgorithms;

import com.google.common.collect.MinMaxPriorityQueue;
import datamodel.GeometryProfile;
import datamodel.Pair;
import datamodel.WeightedGeometry;
import datareader.AbstractReader;
import utilities.WeightingScheme;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.set.TIntSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.jena.atlas.json.JsonArray;
import utilities.IncPairWeightComparator;

public class GeometryOrderedAlgorithm extends IterativeAlgorithm {

    public GeometryOrderedAlgorithm(int budget, int qPairs, AbstractReader sourceReader, 
            AbstractReader targetReader, WeightingScheme wScheme) {
        super(budget, qPairs, sourceReader, targetReader, wScheme);
    }

    public GeometryOrderedAlgorithm(int budget, int qPairs, AbstractReader sourceReader, 
            AbstractReader targetReader, WeightingScheme wScheme, String exportPath) {
        super(budget, qPairs, sourceReader, targetReader, wScheme, exportPath);
    }

    @Override
    public String getMethodName() {
        return "Geometry-ordered Progressive Algorithm";
    }

    @Override
    protected void scheduling() {
        int counter = 0;
        final double[] minimumWeights = new double[datasetDelimiter];
        int localBudget = (int) (2 * Math.ceil(budget / datasetDelimiter));
        final MinMaxPriorityQueue[] localTopKPairs = new MinMaxPriorityQueue[datasetDelimiter];
        for (int i = 0; i < datasetDelimiter; i++) {
            localTopKPairs[i] = MinMaxPriorityQueue.orderedBy(new IncPairWeightComparator()).maximumSize(2 * localBudget).create();
        }

        while (targetReader.hasNext()) {
            counter++;
            GeometryProfile profile = targetReader.next();
            if (profile == null) {
                continue;
            }

            final TIntSet candidateMatches = getCandidates(counter, profile.getGeometry());

            final TIntIterator intIterator = candidateMatches.iterator();
            while (intIterator.hasNext()) {
                int candidateMatchId = intIterator.next();
                if (validCandidate(candidateMatchId, profile.getGeometry().getEnvelopeInternal(), null)) {
                    float weight = getWeight(candidateMatchId, profile.getGeometry().getEnvelopeInternal());
                    if (minimumWeights[candidateMatchId] < weight) {
                        final Pair p = new Pair(candidateMatchId, counter, weight, profile.getGeometry());
                        localTopKPairs[candidateMatchId].add(p);
                        if (localBudget < localTopKPairs[candidateMatchId].size()) {
                            Pair lastPair = (Pair) localTopKPairs[candidateMatchId].poll();
                            minimumWeights[candidateMatchId] = lastPair.getWeight();
                        }
                    }
                }
            }

            counter++;
        }

        final List<WeightedGeometry> sortedSourceGeometries = new ArrayList<>();
        for (int i = 0; i < datasetDelimiter; i++) {
            if (localTopKPairs[i].isEmpty()) {
                continue;
            }
            
            float totalWeight = 0;
            final Iterator<Pair> it = localTopKPairs[i].iterator();
            while (it.hasNext()) {
                Pair p = it.next();
                totalWeight += p.getWeight();
            }
            sortedSourceGeometries.add(new WeightedGeometry(i, totalWeight/localTopKPairs[i].size()));
        }
        Collections.sort(sortedSourceGeometries);
        
        for (WeightedGeometry wGeometry : sortedSourceGeometries) {
            int currentGeomId = wGeometry.getGeometryId();
            while (!localTopKPairs[currentGeomId].isEmpty()) {
                final Pair p = (Pair) localTopKPairs[currentGeomId].pollLast();
                topKPairs.add(p);
                if (topKPairs.size() == budget) {
                    break;
                }
            }
        }
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
