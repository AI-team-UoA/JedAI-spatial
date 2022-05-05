package enums;

import batch.tilebased.GIAnt;
import batch.tilebased.RADON;
import datareader.AbstractReader;

public enum ProgressiveAlgorithms {
    GIANT,
    RADON;

    public static void runAlgorithm(ProgressiveAlgorithms progressiveAlgorithms, AbstractReader sourceReader, AbstractReader targetReader) {
        switch (progressiveAlgorithms) {
            case GIANT:
                GIAnt giant = new GIAnt(0, sourceReader, targetReader);
                giant.applyProcessing();
                giant.printResults();
                break;

            case RADON:
                batch.tilebased.RADON radon = new RADON(0, sourceReader, targetReader);
                radon.applyProcessing();
                radon.printResults();
                break;
        }
    }
    public static void runAlgorithm(ProgressiveAlgorithms progressiveAlgorithms, AbstractReader sourceReader, AbstractReader targetReader, String exportFile) {
        switch (progressiveAlgorithms) {
            case GIANT:
                GIAnt giant = new GIAnt(0, sourceReader, targetReader, exportFile);
                giant.applyProcessing();
                giant.printResults();
                break;

            case RADON:
                batch.tilebased.RADON radon = new RADON(0, sourceReader, targetReader, exportFile);
                radon.applyProcessing();
                radon.printResults();
                break;
        }
    }
}
