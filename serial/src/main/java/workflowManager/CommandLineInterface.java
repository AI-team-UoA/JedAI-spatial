package workflowManager;

import datareader.AbstractReader;
import enums.BatchAlgorithms;
import enums.DataType;
import enums.ProgressiveAlgorithms;

import java.util.Scanner;

import static workflowManager.CommandLineInterfaceUtils.*;

public class CommandLineInterface {

    public static void main(String[] args) {
        System.out.println("Welcome to JedAI-spatial's CLI");

        int exit = 2;
        while (exit == 2) {

            String sourceFilePath = readPath("Please provide the path of your source dataset:");
            int sourceDataType = readInt("Please choose the data types of your source data:", DATA_TYPE);
            AbstractReader sourceReader = DataType.getReader(DataType.values()[sourceDataType-1], sourceFilePath);

            String targetFilePath = readPath("Please provide the path of your target dataset:");
            int targetDataType = readInt("Please choose the data types of your source data:", DATA_TYPE);
            AbstractReader targetReader = DataType.getReader(DataType.values()[targetDataType-1], targetFilePath);

            int toExport = readInt("Please choose whether you want to export your results:", YES_OR_NO);

            String outputFile = null;
            if (toExport == 1) outputFile = readPath("Please provide the path to your export file:");

            int experimentType = readInt("Please choose one of the available Experiments:", EXPERIMENT_TYPE);
            int algorithm;
            switch (experimentType) {
                case 1:
                    algorithm = readInt("Please choose one of the available algorithms:", BATCH_ALGORITHMS);
                    if (toExport == 1)
                        BatchAlgorithms.runAlgorithm(BatchAlgorithms.values()[algorithm-1], sourceReader, targetReader, outputFile);
                    else
                        BatchAlgorithms.runAlgorithm(BatchAlgorithms.values()[algorithm-1], sourceReader, targetReader);

                    break;

                case 2:
                    algorithm = readInt("Please choose one of the available algorithms:", PROGRESSIVE_ALGORITHMS);
                    if (toExport == 1)
                        ProgressiveAlgorithms.runAlgorithm(ProgressiveAlgorithms.values()[algorithm-1], sourceReader, targetReader, outputFile);
                    else
                        ProgressiveAlgorithms.runAlgorithm(ProgressiveAlgorithms.values()[algorithm-1], sourceReader, targetReader);

                    break;
            }

            exit = readInt("Please choose Yes to exit the program or No to perform another experiment:", YES_OR_NO);
        }

    }

}
