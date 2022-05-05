package datareader;

import com.esotericsoftware.minlog.Log;
import datamodel.GeometryProfile;
import exception.InputFileException;
import org.apache.jena.atlas.json.JSON;
import org.apache.jena.atlas.json.JsonArray;
import org.apache.jena.atlas.json.JsonObject;
import org.apache.jena.atlas.json.JsonValue;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import java.util.*;

public class GeometryJSONRDFReader extends AbstractReader {

    private final Set<String> attributesToExclude;
    private final Map<String, GeometryProfile> urlToEntity;
    private String prefix = "";

    private Iterator<JsonValue> jsonIterator;
    private JsonValue nextRecord;
    private final WKTReader wktReader;

    public GeometryJSONRDFReader(String filePath, String prefix, Set<String> attributesToExclude) {
        super(filePath);

        if (inputFilePath == null) {
            Log.error("Input file path has not been set!");
            throw new InputFileException("GeometryJSONRDF Reader input path not given.");
        }

        this.prefix = prefix;
        this.urlToEntity = new HashMap<>();
        this.attributesToExclude = attributesToExclude;
        this.attributesToExclude.add("owl:sameAs");

        // set iterator
        JsonObject jsonObject = JSON.read(inputFilePath);
        String key = jsonObject.keys().toArray()[0].toString();
        JsonArray jarr = jsonObject.get(key).getAsArray();

        jsonIterator = jarr.iterator();
        wktReader = new WKTReader();
    }

    private GeometryProfile readGeometry(String sub, String pred, String obj) {
        //if already exists a profile for the subject, simply add po as <Att>-<Value>
        GeometryProfile geometryProfile = urlToEntity.get(sub);
        if (geometryProfile == null) {
            try {
                Geometry geometry = null;
                geometry = wktReader.read(obj.trim());
                geometryProfile = new GeometryProfile(geometry);
//                geometryProfiles.add(geometryProfile);
                urlToEntity.put(sub, geometryProfile);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }

        if (!obj.isEmpty()) {
            geometryProfile.addAttribute(pred, obj);
        }

        return geometryProfile;
    }

    @Override
    public String getMethodConfiguration() {
        return null;
    }

    @Override
    public String getMethodInfo() {
        return null;
    }

    @Override
    public String getMethodName() {
        return null;
    }

    @Override
    public String getMethodParameters() {
        return null;
    }

    @Override
    public JsonArray getParameterConfiguration() {
        return null;
    }

    @Override
    public String getParameterDescription(int parameterId) {
        return null;
    }

    @Override
    public String getParameterName(int parameterId) {
        return null;
    }

    @Override
    public boolean hasNext() {
        try {
            nextRecord = jsonIterator.next();
            return nextRecord != null;
        } catch (Exception ex) {
            return false;
        }
    }

    @Override
    public GeometryProfile next() {
        final String predicate;
        final String object;
        final String subject;

        try {
            predicate = nextRecord.getAsObject().get("Predicate").getAsString().value();
            object = nextRecord.getAsObject().get("Object").getAsString().value();
            subject = nextRecord.getAsObject().get("Subject").getAsString().value();
        } catch (Exception ex) {
            Log.error("Missing one of predicate, object, subject while reading next line: " + ex.getMessage());
            return null;
        }

        if (attributesToExclude.contains(predicate)) {
            return null;
        }

        counter++;
        return readGeometry(subject, predicate, object);
    }

    @Override
    public void close() {

    }
}
