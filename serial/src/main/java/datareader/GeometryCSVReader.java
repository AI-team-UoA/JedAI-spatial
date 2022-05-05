package datareader;

import com.esotericsoftware.minlog.Log;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import datamodel.GeometryProfile;
import exception.InputFileException;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import org.apache.jena.atlas.json.JsonArray;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import org.apache.jena.atlas.json.JsonObject;

public class GeometryCSVReader extends AbstractReader {

    private final boolean attributeNamesInFirstRow;

    private final char separator;

    private final int geoIndex;

    private CSVReader csvReader;
    private String[] nextRecord;
    private String[] attributeNames;
    private final TIntSet attributesToExclude;
    private final WKTReader wktReader;

    public GeometryCSVReader(boolean attrFirstRow, char separator, int geoIndex, int[] toExclude, String filePath) {
        super(filePath);

        if (inputFilePath == null) {
            Log.error("Input file path has not been set!");
            throw new InputFileException("Geometry CSV Reader input path not given.");
        }

        this.geoIndex = geoIndex;
        this.separator = separator;
        attributeNamesInFirstRow = attrFirstRow;

        wktReader = new WKTReader();

        // attributes to exclude
        attributesToExclude = new TIntHashSet();
        attributesToExclude.add(geoIndex);
        for (int attributeId : toExclude) {
            attributesToExclude.add(attributeId);
        }

        // set iterator
        try {
            BufferedReader br = new BufferedReader(new FileReader(inputFilePath));
            CSVParser parser = new CSVParserBuilder().withSeparator(separator).build();
            csvReader = new CSVReaderBuilder(br).withCSVParser(parser).build();
        } catch (FileNotFoundException ex) {
            Log.error("Error while creating reader : " + ex.getMessage());
            throw new InputFileException("Geometry CSV Reader input path not found.");
        }

        String[] firstRecord = null;
        try {
            firstRecord = csvReader.readNext();
        } catch (IOException ex) {
            Log.error("Error while reading the first row : " + ex.getMessage());
        }

        if (firstRecord == null) {
            Log.error("Empty file given as input.");
            throw new InputFileException("Geometry CSV Reader input path is empty.");
        }

        int noOfAttributes = firstRecord.length;
        if (noOfAttributes - 1 < geoIndex) {
            Log.error("The geometry index does not correspond to a valid column index! Counting starts from 0.");
            System.exit(-1);
        }

        // Setting attribute names
        if (attributeNamesInFirstRow) {
            attributeNames = Arrays.copyOf(firstRecord, noOfAttributes);
        } else { // No attribute names in csv file
            attributeNames = new String[noOfAttributes];
            for (int i = 0; i < noOfAttributes; i++) {
                attributeNames[i] = "attribute" + (i + 1);
            }

            try { // we recrate the reader to ensure that the first geometry is not lost
                csvReader.close();

                BufferedReader br = new BufferedReader(new FileReader(inputFilePath));
                CSVParser parser = new CSVParserBuilder().withSeparator(separator).build();
                csvReader = new CSVReaderBuilder(br).withCSVParser(parser).build();
            } catch (IOException ex) {
                Log.error("Error while re-creating reader : " + ex.getMessage());
                throw new InputFileException("Geometry CSV Reader input path not found.");
            }
        }
    }

    @Override
    public void close() {
        try {
            csvReader.close();
        } catch (IOException ex) {
            Log.warn("Error while closing CSV Reader : " + ex.getMessage());
        }
    }

    @Override
    public String getMethodConfiguration() {
        final StringBuilder sb = new StringBuilder();
        sb.append("{");
        final TIntIterator iterator = attributesToExclude.iterator();
        while (iterator.hasNext()) {
            int attributeId = iterator.next();
            sb.append(attributeId).append(",");
        }
        sb.append("}");

        return getParameterName(0) + "=" + inputFilePath + "\t"
                + getParameterName(1) + "=" + attributeNamesInFirstRow + "\t"
                + getParameterName(2) + "=" + separator + "\t"
                + getParameterName(3) + "=" + geoIndex + "\t"
                + getParameterName(4) + "=" + sb.toString();
    }

    @Override
    public String getMethodInfo() {
        return getMethodName() + ": it converts a CSV file into a set of geometry profiles.";
    }

    @Override
    public String getMethodName() {
        return "CSV Reader";
    }

    @Override
    public String getMethodParameters() {
        return getMethodName() + " involves five parameters:\n"
                + "1)" + getParameterDescription(0) + ".\n"
                + "2)" + getParameterDescription(1) + ".\n"
                + "3)" + getParameterDescription(2) + ".\n"
                + "4)" + getParameterDescription(3) + ".\n"
                + "5)" + getParameterDescription(4) + ".";
    }

    @Override
    public JsonArray getParameterConfiguration() {
        final JsonObject obj1 = new JsonObject();
        obj1.put("class", "java.lang.String");
        obj1.put("name", getParameterName(0));
        obj1.put("defaultValue", "-");
        obj1.put("minValue", "-");
        obj1.put("maxValue", "-");
        obj1.put("stepValue", "-");
        obj1.put("description", getParameterDescription(0));

        final JsonObject obj2 = new JsonObject();
        obj2.put("class", "java.lang.Boolean");
        obj2.put("name", getParameterName(1));
        obj2.put("defaultValue", "true");
        obj2.put("minValue", "-");
        obj2.put("maxValue", "-");
        obj2.put("stepValue", "-");
        obj2.put("description", getParameterDescription(1));

        final JsonObject obj3 = new JsonObject();
        obj3.put("class", "java.lang.Character");
        obj3.put("name", getParameterName(2));
        obj3.put("defaultValue", ",");
        obj3.put("minValue", "-");
        obj3.put("maxValue", "-");
        obj3.put("stepValue", "-");
        obj3.put("description", getParameterDescription(2));

        final JsonObject obj4 = new JsonObject();
        obj4.put("class", "java.lang.Integer");
        obj4.put("name", getParameterName(3));
        obj4.put("defaultValue", "0");
        obj4.put("minValue", "0");
        obj4.put("maxValue", "-");
        obj4.put("stepValue", "-");
        obj4.put("description", getParameterDescription(3));

        final JsonObject obj5 = new JsonObject();
        obj5.put("class", "gnu.trove.set.TIntSet");
        obj5.put("name", getParameterName(4));
        obj5.put("defaultValue", "-");
        obj5.put("minValue", "-");
        obj5.put("maxValue", "-");
        obj5.put("stepValue", "-");
        obj5.put("description", getParameterDescription(4));

        final JsonArray array = new JsonArray();
        array.add(obj1);
        array.add(obj2);
        array.add(obj3);
        array.add(obj4);
        array.add(obj5);
        return array;
    }

    @Override
    public String getParameterDescription(int parameterId) {
        switch (parameterId) {
            case 0:
                return "The " + getParameterName(0) + " determines the absolute path to the CSV file that will be read into main memory.";
            case 1:
                return "The " + getParameterName(1) + " determines whether the first line of the CSV file contains the attribute names (true), "
                        + "or it contains an entity profile (false).";
            case 2:
                return "The " + getParameterName(2) + " determines the character used to tokenize every line into attribute values.";
            case 3:
                return "The " + getParameterName(3) + " determines the number of column/attribute (starting from 0) that contains the geometry in WKT form. "
                        + "If the given id is larger than the number of columns, an exception is thrown. ";
            case 4:
                return "The " + getParameterName(4) + " specifies the column ids (in the form of comma-separated integers) that will be ignored "
                        + "during the creation of entity profiles.";
            default:
                return "invalid parameter id";
        }
    }

    @Override
    public String getParameterName(int parameterId) {
        switch (parameterId) {
            case 0:
                return "File Path";
            case 1:
                return "Attribute Names In First Row";
            case 2:
                return "Separator";
            case 3:
                return "Geometry index";
            case 4:
                return "Attributes To Exclude";
            default:
                return "invalid parameter id";
        }
    }

    @Override
    public boolean hasNext() {
        try {
            nextRecord = csvReader.readNext();
            return nextRecord != null;
        } catch (IOException ex) {
            Log.error("Error while reading next line : " + ex.getMessage());
            return false;
        }
    }

    @Override
    public GeometryProfile next() {
        if (nextRecord.length < attributeNames.length - 1) {
            Log.warn("Line with missing attribute names : " + Arrays.toString(nextRecord));
            return null;
        } else if (attributeNames.length < nextRecord.length) {
            Log.warn("Line with more attributes : " + Arrays.toString(nextRecord));
            return null;
        }

        Geometry geometry = null;
        try {
            geometry = wktReader.read(nextRecord[geoIndex].trim());
        } catch (ParseException ex) {
            Log.error("Error while reading geometry : " + ex.getMessage());
        }

        if (geometry == null) {
            Log.error("Null geometry was read!");
            return null;
        }
        
        if (geometry.getGeometryType().equals("GeometryCollection")) {
            Log.error("Geometry collection was read!");
            return null;
        }

        final GeometryProfile newProfile = new GeometryProfile(geometry);
        for (int i = 0; i < attributeNames.length; i++) {
            if (attributesToExclude.contains(i)) {
                continue;
            }

            if (!nextRecord[i].trim().isEmpty()) {
                newProfile.addAttribute(attributeNames[i], nextRecord[i]);
            }
        }

        counter++;
        return newProfile;
    }
}
