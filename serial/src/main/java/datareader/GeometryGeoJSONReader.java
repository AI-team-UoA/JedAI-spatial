package datareader;

import com.esotericsoftware.minlog.Log;
import datamodel.GeometryProfile;
import exception.InputFileException;
import org.apache.jena.atlas.json.JSON;
import org.apache.jena.atlas.json.JsonArray;
import org.apache.jena.atlas.json.JsonObject;
import org.apache.jena.atlas.json.JsonValue;
import org.geotools.geojson.geom.GeometryJSON;
import org.locationtech.jts.geom.Geometry;
import java.util.Iterator;

public class GeometryGeoJSONReader extends AbstractReader{

    private Iterator<JsonValue> jsonIterator;
    private JsonValue nextRecord;
    private final GeometryJSON geometryJSON;

    public GeometryGeoJSONReader(String filePath) {
        super(filePath);

        if (inputFilePath == null) {
            Log.error("Input file path has not been set!");
            throw new InputFileException("GeometryJSONRDF Reader input path not given.");
        }

        // set iterator
        final JsonObject jsonObject = JSON.read(inputFilePath);
        final JsonArray features = jsonObject.get("features").getAsArray();
        jsonIterator = features.iterator();
        geometryJSON = new GeometryJSON();
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
        String geometryStr = nextRecord.getAsObject().get("geometry")
                .getAsObject().toString();

        GeometryProfile geometryProfile;
        try {
            Geometry geometry = geometryJSON.read(geometryStr);
            geometryProfile = new GeometryProfile(geometry);
        } catch (Exception ex) {
            Log.error("GeoTools: Error when reading geometry." + ex.getMessage());
            return null;
        }

        counter++;
        return geometryProfile;
    }

    @Override
    public void close() {

    }
}
