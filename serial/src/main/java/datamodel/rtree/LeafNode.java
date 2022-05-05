package datamodel.rtree;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class LeafNode extends Node {

    public LeafNode(int maxDegree) {
        super(maxDegree);
    }

    public LeafNode(Node node) {
        super(node);
    }

}
