package workflowManager;

import java.util.Scanner;

public class CommandLineInterfaceUtils {

    public static int readInt(String message, String[] array) {
        System.out.println("\n" + message);
        for (int i = 0; i < array.length; i++) {
            System.out.println((i + 1) + " - " + array[i]);
        }

        int userInt;
        while (true) {
            Scanner keyboard = new Scanner(System.in);

            try {
                userInt = keyboard.nextInt();
            } catch (Exception ex) {
                System.out.println("Invalid input. Please choose between 1 and " + array.length);
                continue;
            }

            if (userInt < 1 || userInt > array.length) {
                System.out.println("Invalid input. Please choose between 1 and " + array.length);
                continue;
            }

            break;
        }

        System.out.println(array[userInt - 1] + " has been selected!");
        return userInt;
    }

    public static int readInt(String message) {
        System.out.println("\n" + message);
        Scanner keyboard = new Scanner(System.in);
        return keyboard.nextInt();
    }

    public static String readPath(String message) {
        System.out.println("\n" + message);
        Scanner scanner = new Scanner(System.in);
        return scanner.nextLine();
    }

    public final static String[] DATA_TYPE = {"CSV", "TSV", "GeoJSON", "JSONRDF", "RDF"};
    public final static String[] YES_OR_NO = {"Yes", "No"};
    public final static String[] EXPERIMENT_TYPE = {"Batch", "Progressive"};
    public final static String[] BATCH_ALGORITHMS = {
            "GIA.nt", "RADON", "Plane Sweep (List)",
            "Plane Sweep (Strips)", "PBSM (List)",
            "PBSM (Strips)", "R-Tree", "Quad Tree",
            "CR-Tree", "Strip Sweep", "Strip STR Sweep"};
    public final static String[] PROGRESSIVE_ALGORITHMS = {"GIA.nt", "RADON"};

}
