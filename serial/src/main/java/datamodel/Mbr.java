package datamodel;

import lombok.Getter;
import lombok.Setter;
import org.locationtech.jts.geom.Envelope;

@Getter
@Setter
public class Mbr {

    int index;
    Envelope envelope;

    public Mbr() {
        this.index = -1;
        this.envelope = new Envelope();
    }

    public Mbr(Envelope mbr, int index) {
        this.index = index;
        this.envelope = mbr;
    }

    public double getMinX() {
        return this.envelope.getMinX();
    }

    public double getMaxX() {
        return this.envelope.getMaxX();
    }

    public double getMinY() {
        return this.envelope.getMinY();
    }

    public double getMaxY() {
        return this.envelope.getMaxY();
    }
}
