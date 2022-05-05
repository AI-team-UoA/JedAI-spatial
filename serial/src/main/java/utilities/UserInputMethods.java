package utilities;

import datareader.AbstractReader;
import datareader.GeometryCSVReader;
import datareader.GeometryGeoJSONReader;
import datareader.GeometrySerializationReader;
import enums.InputDataType;
import javax.swing.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;

public class UserInputMethods {

    protected AbstractReader pickReaderClass(InputDataType dataType, String sourceFilePath, char delimiter) {
        switch (dataType) {
            case CSV:
                return new GeometryCSVReader(true, delimiter, -1, new int[]{}, sourceFilePath);
            case RDF:
//                return new GeometryRDFReader(sourceFilePath);
            case GeoJSON:
                return new GeometryGeoJSONReader(sourceFilePath);
            case JSONRDF:
//                return new GeometryJSONRDFReader(sourceFilePath, "");
            case Serialization:
                return new GeometrySerializationReader(sourceFilePath);
        }
        return null;
    }
    
    // reads a file path from the user
    public static String readPath() {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter absolute file path: ");
        String filePath = scanner.nextLine();
        Path path = Paths.get(filePath);

        return path.toString();
    }

    // reads the type of file the user submitted
    public static InputDataType readFileType() {
        JDialog dialog = new JDialog();
        dialog.setAlwaysOnTop(true);
        return (InputDataType) JOptionPane.showInputDialog(dialog,
                "Choose the type of your file", "Data Type",
                JOptionPane.INFORMATION_MESSAGE, null,
                InputDataType.values(), InputDataType.values()[0]);
    }
}
