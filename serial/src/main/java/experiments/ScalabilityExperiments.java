package experiments;

import batch.partitionbased.PBSM;
import batch.planesweep.PlaneSweep;
import batch.stripebased.StripeSweep;
import batch.stripebased.StripeSTRSweep;
import batch.tilebased.GIAnt;
import batch.tilebased.RADON;
import batch.tilebased.StaticGIAnt;
import batch.tilebased.StaticRADON;
import batch.treebased.CRTree;
import batch.treebased.QuadTree;
import batch.treebased.RTree;
import datareader.AbstractReader;
import datareader.GeometryCSVReader;
import enums.PlaneSweepStructure;

public class ScalabilityExperiments {

    private final static int ITERATIONS = 5;
    private final static int QUALIFYING_PAIRS = 2401396;
    
    public static void main(String[] args) {
        String mainDir = "/data/geometries/samples/s";
        
        for (int sampleId = 0; sampleId < 10; sampleId++) {
            System.out.println("\n\n\nCurrent method\t:\tStatic GIAnt");
            for (int it = 0; it < ITERATIONS; it++) {
                AbstractReader sourceReader = new GeometryCSVReader(false, '\t', 0, new int[]{1}, mainDir + (sampleId + 1) + "/sourceSample.tsv");
                AbstractReader targetReader = new GeometryCSVReader(false, '\t', 0, new int[]{1}, mainDir + (sampleId + 1) + "/targetSample.tsv");

                StaticGIAnt sGIAnt = new StaticGIAnt(QUALIFYING_PAIRS, sourceReader, targetReader);
                sGIAnt.applyProcessing();
                sGIAnt.printResults();
            }
                        
            System.out.println("\n\n\nCurrent method\t:\tGIA.nt");
            for (int it = 0; it < ITERATIONS; it++) {
                AbstractReader sourceReader = new GeometryCSVReader(false, '\t', 0, new int[]{1}, mainDir + (sampleId + 1) + "/sourceSample.tsv");
                AbstractReader targetReader = new GeometryCSVReader(false, '\t', 0, new int[]{1}, mainDir + (sampleId + 1) + "/targetSample.tsv");

                GIAnt giant = new GIAnt(QUALIFYING_PAIRS, sourceReader, targetReader);
                giant.applyProcessing();
                giant.printResults();
            }
            
            System.out.println("\n\n\nCurrent method\t:\tStatic RADON");
            for (int it = 0; it < ITERATIONS; it++) {
                AbstractReader sourceReader = new GeometryCSVReader(false, '\t', 0, new int[]{1}, mainDir + (sampleId + 1) + "/sourceSample.tsv");
                AbstractReader targetReader = new GeometryCSVReader(false, '\t', 0, new int[]{1}, mainDir + (sampleId + 1) + "/targetSample.tsv");

                StaticRADON sRadon = new StaticRADON(QUALIFYING_PAIRS, sourceReader, targetReader);
                sRadon.applyProcessing();
                sRadon.printResults();
            }

            System.out.println("\n\n\nCurrent method\t:\tRADON");
            for (int it = 0; it < ITERATIONS; it++) {
                AbstractReader sourceReader = new GeometryCSVReader(false, '\t', 0, new int[]{1}, mainDir + (sampleId + 1) + "/sourceSample.tsv");
                AbstractReader targetReader = new GeometryCSVReader(false, '\t', 0, new int[]{1}, mainDir + (sampleId + 1) + "/targetSample.tsv");

                RADON radon = new RADON(QUALIFYING_PAIRS, sourceReader, targetReader);
                radon.applyProcessing();
                radon.printResults();
            }

            System.out.println("\n\n\nCurrent method\t:\tPlaneSweep+List");
            for (int it = 0; it < ITERATIONS; it++) {
                AbstractReader sourceReader = new GeometryCSVReader(false, '\t', 0, new int[]{1}, mainDir + (sampleId + 1) + "/sourceSample.tsv");
                AbstractReader targetReader = new GeometryCSVReader(false, '\t', 0, new int[]{1}, mainDir + (sampleId + 1) + "/targetSample.tsv");

                PlaneSweep ps = new PlaneSweep(QUALIFYING_PAIRS, sourceReader, targetReader, PlaneSweepStructure.LIST_SWEEP);
                ps.applyProcessing();
                ps.printResults();
            }

            System.out.println("\n\n\nCurrent method\t:\tPlaneSweep+Stripes");
            for (int it = 0; it < ITERATIONS; it++) {
                AbstractReader sourceReader = new GeometryCSVReader(false, '\t', 0, new int[]{1}, mainDir + (sampleId + 1) + "/sourceSample.tsv");
                AbstractReader targetReader = new GeometryCSVReader(false, '\t', 0, new int[]{1}, mainDir + (sampleId + 1) + "/targetSample.tsv");

                PlaneSweep ps = new PlaneSweep(QUALIFYING_PAIRS, sourceReader, targetReader, PlaneSweepStructure.STRIPED_SWEEP);
                ps.applyProcessing();
                ps.printResults();
            }

            System.out.println("\n\n\nCurrent method\t:\tPBSM+List");
            for (int it = 0; it < ITERATIONS; it++) {
                AbstractReader sourceReader = new GeometryCSVReader(false, '\t', 0, new int[]{1}, mainDir + (sampleId + 1) + "/sourceSample.tsv");
                AbstractReader targetReader = new GeometryCSVReader(false, '\t', 0, new int[]{1}, mainDir + (sampleId + 1) + "/targetSample.tsv");

                PBSM pbsm = new PBSM(QUALIFYING_PAIRS, sourceReader, targetReader, PlaneSweepStructure.LIST_SWEEP);
                pbsm.applyProcessing();
                pbsm.printResults();
            }

            System.out.println("\n\n\nCurrent method\t:\tPBSM+Stripes");
            for (int it = 0; it < ITERATIONS; it++) {
                AbstractReader sourceReader = new GeometryCSVReader(false, '\t', 0, new int[]{1}, mainDir + (sampleId + 1) + "/sourceSample.tsv");
                AbstractReader targetReader = new GeometryCSVReader(false, '\t', 0, new int[]{1}, mainDir + (sampleId + 1) + "/targetSample.tsv");

                PBSM pbsm = new PBSM(QUALIFYING_PAIRS, sourceReader, targetReader, PlaneSweepStructure.STRIPED_SWEEP);
                pbsm.applyProcessing();
                pbsm.printResults();
            }

            System.out.println("\n\n\nCurrent method\t:\tStripe Sweep");
            for (int it = 0; it < ITERATIONS; it++) {
                AbstractReader sourceReader = new GeometryCSVReader(false, '\t', 0, new int[]{1}, mainDir + (sampleId + 1) + "/sourceSample.tsv");
                AbstractReader targetReader = new GeometryCSVReader(false, '\t', 0, new int[]{1}, mainDir + (sampleId + 1) + "/targetSample.tsv");

                StripeSweep ss = new StripeSweep(QUALIFYING_PAIRS, sourceReader, targetReader);
                ss.applyProcessing();
                ss.printResults();
            }

            System.out.println("\n\n\nCurrent method\t:\tStripe STR Sweep");
            for (int it = 0; it < ITERATIONS; it++) {
                AbstractReader sourceReader = new GeometryCSVReader(false, '\t', 0, new int[]{1}, mainDir + (sampleId + 1) + "/sourceSample.tsv");
                AbstractReader targetReader = new GeometryCSVReader(false, '\t', 0, new int[]{1}, mainDir + (sampleId + 1) + "/targetSample.tsv");

                StripeSTRSweep sss = new StripeSTRSweep(QUALIFYING_PAIRS, sourceReader, targetReader);
                sss.applyProcessing();
                sss.printResults();
            }

            System.out.println("\n\n\nCurrent method\t:\tCRTree");
            for (int it = 0; it < ITERATIONS; it++) {
                AbstractReader sourceReader = new GeometryCSVReader(false, '\t', 0, new int[]{1}, mainDir + (sampleId + 1) + "/sourceSample.tsv");
                AbstractReader targetReader = new GeometryCSVReader(false, '\t', 0, new int[]{1}, mainDir + (sampleId + 1) + "/targetSample.tsv");

                CRTree crTree = new CRTree(QUALIFYING_PAIRS, sourceReader, targetReader);
                crTree.applyProcessing();
                crTree.printResults();
            }

            System.out.println("\n\n\nCurrent method\t:\tQuadTree");
            for (int it = 0; it < ITERATIONS; it++) {
                AbstractReader sourceReader = new GeometryCSVReader(false, '\t', 0, new int[]{1}, mainDir + (sampleId + 1) + "/sourceSample.tsv");
                AbstractReader targetReader = new GeometryCSVReader(false, '\t', 0, new int[]{1}, mainDir + (sampleId + 1) + "/targetSample.tsv");

                QuadTree qdTree = new QuadTree(QUALIFYING_PAIRS, sourceReader, targetReader);
                qdTree.applyProcessing();
                qdTree.printResults();
            }

            System.out.println("\n\n\nCurrent method\t:\tRTree");
            for (int it = 0; it < ITERATIONS; it++) {
                AbstractReader sourceReader = new GeometryCSVReader(false, '\t', 0, new int[]{1}, mainDir + (sampleId + 1) + "/sourceSample.tsv");
                AbstractReader targetReader = new GeometryCSVReader(false, '\t', 0, new int[]{1}, mainDir + (sampleId + 1) + "/targetSample.tsv");

                RTree rTree = new RTree(QUALIFYING_PAIRS, sourceReader, targetReader);
                rTree.applyProcessing();
                rTree.printResults();
            }
        }
    }
}
