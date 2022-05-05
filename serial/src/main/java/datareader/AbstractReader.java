package datareader;

import com.esotericsoftware.minlog.Log;
import datamodel.GeometryProfile;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public abstract class AbstractReader implements IGeometryReader, Iterator<GeometryProfile> {

    protected int counter;
    
    protected final List<GeometryProfile> geometryProfiles;
    protected final String inputFilePath;

    public AbstractReader(String filePath) {
        counter = 0;
        inputFilePath = filePath;
        this.geometryProfiles = new ArrayList<>();
    }
  
    @Override
    public GeometryProfile[] getGeometryProfiles() {
        while (hasNext()) {
            GeometryProfile nextProfile = next();
            if (nextProfile != null) {
                geometryProfiles.add(nextProfile);
            }
        }
        close();

        return geometryProfiles.toArray(new GeometryProfile[geometryProfiles.size()]);
    }

    public int getSize() {
        return geometryProfiles.isEmpty() ? counter : geometryProfiles.size();
    }
    
    @Override
    public void storeSerializedObject(Object object, String outputPath) {
        try {
            final OutputStream file = new FileOutputStream(outputPath);
            final OutputStream buffer = new BufferedOutputStream(file);
            try ( ObjectOutput output = new ObjectOutputStream(buffer)) {
                output.writeObject(object);
            }
        } catch (IOException ioex) {
            Log.error("Error in storing serialized object", ioex);
        }
    }

    public abstract void close();
}
