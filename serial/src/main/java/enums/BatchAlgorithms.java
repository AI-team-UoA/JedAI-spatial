package enums;

import batch.partitionbased.PBSM;
import batch.planesweep.PlaneSweep;
import batch.stripebased.StripeSTRSweep;
import batch.stripebased.StripeSweep;
import batch.tilebased.GIAnt;
import batch.tilebased.RADON;
import batch.treebased.CRTree;
import batch.treebased.QuadTree;
import batch.treebased.RTree;
import datareader.AbstractReader;
import enums.PlaneSweepStructure;

public enum BatchAlgorithms {
    GIANT,
    RADON,
    PLANE_SWEEP_LIST,
    PLANE_SWEEP_STRIP,
    PBSM_LIST,
    PBSM_STRIP,
    R_TREE,
    QUAD_TREE,
    CR_TREE,
    STRIP_SWEEP,
    STRIP_STR_SWEEP;

    public static void runAlgorithm(BatchAlgorithms batchAlgorithms, AbstractReader sourceReader, AbstractReader targetReader) {
        switch (batchAlgorithms) {
            case GIANT:
                GIAnt giant = new GIAnt(0, sourceReader, targetReader);
                giant.applyProcessing();
                giant.printResults();
                break;

            case RADON:
                RADON radon = new RADON(0, sourceReader, targetReader);
                radon.applyProcessing();
                radon.printResults();
                break;

            case PLANE_SWEEP_LIST:
                PlaneSweep planeSweepList = new PlaneSweep(0, sourceReader, targetReader, PlaneSweepStructure.LIST_SWEEP);
                planeSweepList.applyProcessing();
                planeSweepList.printResults();
                break;

            case PLANE_SWEEP_STRIP:
                PlaneSweep planeSweepStrip = new PlaneSweep(0, sourceReader, targetReader, PlaneSweepStructure.STRIPED_SWEEP);
                planeSweepStrip.applyProcessing();
                planeSweepStrip.printResults();
                break;

            case PBSM_LIST:
                PBSM pbsmList = new PBSM(0, sourceReader, targetReader, PlaneSweepStructure.LIST_SWEEP);
                pbsmList.applyProcessing();
                pbsmList.printResults();
                break;

            case PBSM_STRIP:
                PBSM pbsmStrip = new PBSM(0, sourceReader, targetReader, PlaneSweepStructure.LIST_SWEEP);
                pbsmStrip.applyProcessing();
                pbsmStrip.printResults();
                break;

            case R_TREE:
                RTree rTree = new RTree(0, sourceReader, targetReader);
                rTree.applyProcessing();
                rTree.printResults();
                break;

            case CR_TREE:
                CRTree crTree = new CRTree(0, sourceReader, targetReader);
                crTree.applyProcessing();
                crTree.printResults();
                break;

            case QUAD_TREE:
                QuadTree qdTree = new QuadTree(0, sourceReader, targetReader);
                qdTree.applyProcessing();
                qdTree.printResults();
                break;

            case STRIP_SWEEP:
                StripeSweep stripeSweep = new StripeSweep(0, sourceReader, targetReader);
                stripeSweep.applyProcessing();
                stripeSweep.printResults();
                break;

            case STRIP_STR_SWEEP:
                StripeSTRSweep stripeSTRSweep = new StripeSTRSweep(0, sourceReader, targetReader);
                stripeSTRSweep.applyProcessing();
                stripeSTRSweep.printResults();
                break;
        }
    }

    public static void runAlgorithm(BatchAlgorithms batchAlgorithms, AbstractReader sourceReader, AbstractReader targetReader, String exportFile) {
        switch (batchAlgorithms) {
            case GIANT:
                GIAnt giant = new GIAnt(0, sourceReader, targetReader, exportFile);
                giant.applyProcessing();
                giant.printResults();
                break;

            case RADON:
                RADON radon = new RADON(0, sourceReader, targetReader, exportFile);
                radon.applyProcessing();
                radon.printResults();
                break;

            case PLANE_SWEEP_LIST:
                PlaneSweep planeSweepList = new PlaneSweep(0, sourceReader, targetReader, PlaneSweepStructure.LIST_SWEEP, exportFile);
                planeSweepList.applyProcessing();
                planeSweepList.printResults();
                break;

            case PLANE_SWEEP_STRIP:
                PlaneSweep planeSweepStrip = new PlaneSweep(0, sourceReader, targetReader, PlaneSweepStructure.STRIPED_SWEEP, exportFile);
                planeSweepStrip.applyProcessing();
                planeSweepStrip.printResults();
                break;

            case PBSM_LIST:
                PBSM pbsmList = new PBSM(0, sourceReader, targetReader, PlaneSweepStructure.LIST_SWEEP, exportFile);
                pbsmList.applyProcessing();
                pbsmList.printResults();
                break;

            case PBSM_STRIP:
                PBSM pbsmStrip = new PBSM(0, sourceReader, targetReader, PlaneSweepStructure.LIST_SWEEP, exportFile);
                pbsmStrip.applyProcessing();
                pbsmStrip.printResults();
                break;

            case R_TREE:
                RTree rTree = new RTree(0, sourceReader, targetReader, exportFile);
                rTree.applyProcessing();
                rTree.printResults();
                break;

            case CR_TREE:
                CRTree crTree = new CRTree(0, sourceReader, targetReader, exportFile);
                crTree.applyProcessing();
                crTree.printResults();
                break;

            case QUAD_TREE:
                QuadTree qdTree = new QuadTree(0, sourceReader, targetReader, exportFile);
                qdTree.applyProcessing();
                qdTree.printResults();
                break;

            case STRIP_SWEEP:
                StripeSweep stripeSweep = new StripeSweep(0, sourceReader, targetReader, exportFile);
                stripeSweep.applyProcessing();
                stripeSweep.printResults();
                break;

            case STRIP_STR_SWEEP:
                StripeSTRSweep stripeSTRSweep = new StripeSTRSweep(0, sourceReader, targetReader, exportFile);
                stripeSTRSweep.applyProcessing();
                stripeSTRSweep.printResults();
                break;
        }
    }
}
