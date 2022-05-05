package datareader;

import datamodel.GeometryProfile;
import utilities.IDocumentation;

public interface IGeometryReader extends IDocumentation {
    
    GeometryProfile[] getGeometryProfiles();
    
    void storeSerializedObject(Object object, String outputPath);
}
