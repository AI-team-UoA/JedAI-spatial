package datawriter;

import com.esotericsoftware.minlog.Log;
import lombok.Getter;
import lombok.Setter;
import org.apache.jena.tdb.base.file.FileException;

import java.io.BufferedWriter;
import java.io.FileWriter;

@Getter
@Setter
public class RDFWriter {

    private final BufferedWriter writer;

    private String contains = "<http://www.opengis.net/ont/geosparql#sfContains>";
    private String coveredBy = "<http://www.opengis.net/ont/geosparql#sfCoverdBy>";
    private String covers = "<http://www.opengis.net/ont/geosparql#sfCovers>";
    private String crosses = "<http://www.opengis.net/ont/geosparql#sfCrosses>";
    private String equals = "<http://www.opengis.net/ont/geosparql#sfEquals>";
    private String intersects = "<http://www.opengis.net/ont/geosparql#sfIntersects>";
    private String overlaps = "<http://www.opengis.net/ont/geosparql#sfOverlaps>";
    private String touches = "<http://www.opengis.net/ont/geosparql#sfTouches>";
    private String within = "<http://www.opengis.net/ont/geosparql#sfWithin>";


    public RDFWriter(String path)  {
        try {
            this.writer = new BufferedWriter(new FileWriter(path));
        } catch(Exception ex) {
            Log.error("Something went wrong when creating an output file");
            throw new FileException("RDF Export File Creation Exception");
        }
    }

    public void exportPair(int source, int target, String relation) {
        try {
            writer.append("<" + source +">" + " " + relation + " " + "<" + target + ">" + " .\n");
        } catch(Exception ex) {
            Log.error("Something went wrong when exporting pair as an RDF triple");
            throw new FileException("RDF Export File Exception");
        }
    }

    public void close() {
        try {
            writer.close();
        } catch (Exception ex) {
            Log.error("Something went wrong when closing RDF writer");
            throw new FileException("RDF Writer Close Exception");
        }
    }

}
