package datamodel;

import java.io.Serializable;
import java.util.Objects;

public class Attribute implements Serializable {

    private static final long serialVersionUID = 1245324342344634589L;

    private final String name;
    private final String value;

    public Attribute (String nm, String val) {
        name = nm;
        value = val;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Attribute other = (Attribute) obj;
        if (!Objects.equals(this.name, other.name)) {
            return false;
        }
        return Objects.equals(this.value, other.value);
    }

    public String getName() {
        return name;
    }

    public String getValue() {
        return value;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 59 * hash + (this.name != null ? this.name.hashCode() : 0);
        hash = 59 * hash + (this.value != null ? this.value.hashCode() : 0);
        return hash;
    }

    @Override
    public String toString() {
        return "attribute\t:\t" + name + ", value\t:\t" + value;
    }
}