/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package experiments;

import datareader.AbstractReader;
import datareader.GeometryCSVReader;
import java.io.IOException;
import progressiveAlgorithms.ProgressiveGIAnt;
import utilities.WeightingScheme;

public class TestProgressiveGIAnt {

    public static void main(String[] args) throws IOException {
        int budget = 1000;
        int qPairs = 648;
        String mainDir = "../data/";
        String dataset1 = "regions_gr.csv";
        String dataset2 = "wildlife_sanctuaries.csv";

        for (WeightingScheme wScheme : WeightingScheme.values()) {
            AbstractReader sourceReader = new GeometryCSVReader(true, ',', 0, new int[]{1}, mainDir + dataset1);
            AbstractReader targetReader = new GeometryCSVReader(true, ',', 0, new int[]{1}, mainDir + dataset2);

            System.out.println("\n\nBudget\t:\t" + budget);
            System.out.println("Dataset\t:\t" + dataset1 + "," + dataset2);
            System.out.println("Weighting scheme\t:\t" + wScheme);

            ProgressiveGIAnt alg = new ProgressiveGIAnt(budget, qPairs, sourceReader, targetReader, wScheme, mainDir + "output.rdf");
            alg.applyProcessing();
            alg.printResults();
        }
    }
}
