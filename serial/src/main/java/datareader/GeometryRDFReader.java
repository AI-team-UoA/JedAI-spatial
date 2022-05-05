package datareader;

import com.esotericsoftware.minlog.Log;
import datamodel.GeometryProfile;
import exception.InputFileException;
import org.apache.jena.atlas.json.JsonArray;
import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.RDFDataMgr;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import java.util.*;

public class GeometryRDFReader extends AbstractReader {

    private final Set<String> attributesToExclude;
//    private final Map<String, GeometryProfile> urlToEntity;
    private final String prefix = "";

    final StmtIterator iterator;
    final WKTReader wktReader;
    private Statement nextStatement;

    public GeometryRDFReader(String filePath, Set<String> attributesToExclude) {
        super(filePath);

        if (inputFilePath == null) {
            Log.error("Input file path has not been set!");
            throw new InputFileException("Geometry CSV Reader input path not given.");
        }

//        urlToEntity = new HashMap<>();
        this.attributesToExclude = attributesToExclude;
        this.attributesToExclude.add("owl:sameAs");

        wktReader = new WKTReader();

        final Model model = RDFDataMgr.loadModel(inputFilePath);
        iterator = model.listStatements();
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
            nextStatement = iterator.nextStatement();
            return nextStatement != null;
        } catch (Exception ex) {
            return false;
        }
    }

    @Override
    public GeometryProfile next() {
        final Property predicate = nextStatement.getPredicate();
        final String pred = predicate.toString();
        if (attributesToExclude.contains(pred)) {
            return null;
        }

        final Resource subject = nextStatement.getSubject();
        String sub = subject.toString();
        if (!prefix.equals("")) {
            sub = sub.replace(prefix, "");
        }

        final RDFNode object = nextStatement.getObject();
        final String objectStr = object.toString();
        final String[] objects = objectStr.split(">");
        if (objects.length < 2) {
            return null;
        }

        GeometryProfile geometryProfile = null;
        try {
            Geometry geometry = wktReader.read(objects[1].trim());
            geometryProfile = new GeometryProfile(geometry);

            if (!geometry.isEmpty()) {
                geometryProfile.addAttribute(pred, objectStr);
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }

        counter++;
        return geometryProfile;
    }

    @Override
    public void close() {
        iterator.close();
    }
}
