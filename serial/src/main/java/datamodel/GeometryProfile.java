package datamodel;

import org.locationtech.jts.geom.Geometry;
import enums.Axis;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class GeometryProfile implements Serializable {

    private final Geometry geometry;
    private final Set<Attribute> attributes;

    public GeometryProfile(Geometry geometry) {
        this.geometry = geometry;
        attributes = new HashSet<>();
    }

    public Geometry getGeometry() {
        return geometry;
    }

    public void addAttribute(String propertyName, String propertyValue) {
        attributes.add(new Attribute(propertyName, propertyValue));
    }

    public boolean isLessThan(GeometryProfile geometryProfile, Axis axis) {
        if (axis == Axis.X_AXIS)
            return geometry.getEnvelopeInternal().getMinX() < geometryProfile.geometry.getEnvelopeInternal().getMinX();
        else
            return  geometry.getEnvelopeInternal().getMinY() < geometryProfile.geometry.getEnvelopeInternal().getMinY();
    }

    public boolean isGreaterThan(GeometryProfile geometryProfile, Axis axis) {
        if (axis == Axis.X_AXIS)
            return geometry.getEnvelopeInternal().getMinX() > geometryProfile.geometry.getEnvelopeInternal().getMinX();
        else
            return geometry.getEnvelopeInternal().getMinY() > geometryProfile.geometry.getEnvelopeInternal().getMinY();
    }

    public double getMaxX() {
        return geometry.getEnvelopeInternal().getMaxX();
    }

    public double getMinX() {
        return geometry.getEnvelopeInternal().getMinX();
    }

    public double getMaxY() {
        return geometry.getEnvelopeInternal().getMaxY();
    }

    public double getMinY() {
        return geometry.getEnvelopeInternal().getMinY();
    }
}
