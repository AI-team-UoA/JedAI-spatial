package utilities;

import org.apache.jena.atlas.json.JsonArray;

public interface IDocumentation {

    String PARAMETER_FREE = "Parameter-free method";

    String getMethodConfiguration();

    String getMethodInfo();

    String getMethodName();

    String getMethodParameters();

    JsonArray getParameterConfiguration();

    String getParameterDescription(int parameterId);

    String getParameterName(int parameterId);
}
