package datareader;

import com.esotericsoftware.minlog.Log;
import datamodel.GeometryProfile;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import org.apache.jena.atlas.json.JsonArray;

import java.util.List;

public class GeometrySerializationReader extends AbstractReader {

    public GeometrySerializationReader(String filePath) {
        super(filePath);
    }

    @Override
    public GeometryProfile[] getGeometryProfiles() {
        if (!geometryProfiles.isEmpty()) {
            return this.geometryProfiles.toArray(new GeometryProfile[geometryProfiles.size()]);
        }

        if (inputFilePath == null) {
            Log.error("Input file path has not been set!");
            return null;
        }

        geometryProfiles.addAll((List<GeometryProfile>) loadSerializedObject(inputFilePath));
        return this.geometryProfiles.toArray(new GeometryProfile[geometryProfiles.size()]);
    }

    @Override
    public String getMethodConfiguration() {
        return null;
    }

    @Override
    public String getMethodInfo() {
        return getMethodName() + ": it loads a file with Java serialized GeometryProfile objects into memory.";
    }

    @Override
    public String getMethodName() {
        return "Serialization Reader";
    }

    @Override
    public String getMethodParameters() {
        // TODO
        return null;
    }

    @Override
    public JsonArray getParameterConfiguration() {
        // TODO
        return null;
    }

    @Override
    public String getParameterDescription(int parameterId) {
        // TODO
        return null;
    }

    @Override
    public String getParameterName(int parameterId) {
        // TODO
        return null;
    }
    
    private static Object loadSerializedObject(String fileName) {
        Object object;
        try {
            final InputStream file = new FileInputStream(fileName);
            final InputStream buffer = new BufferedInputStream(file);
            try (ObjectInput input = new ObjectInputStream(buffer)) {
                object = input.readObject();
            }
            file.close();
        } catch (ClassNotFoundException cnfEx) {
            Log.error("Missing class", cnfEx);
            return null;
        } catch (IOException ioex) {
            Log.error("Error in data reading", ioex);
            return null;
        }
        return object;
    }

    @Override
    public boolean hasNext() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public GeometryProfile next() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void close() {
        
    }
}
