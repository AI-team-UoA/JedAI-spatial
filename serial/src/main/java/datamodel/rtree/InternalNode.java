package datamodel.rtree;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class InternalNode extends Node{


    public InternalNode(int maxDegree) {
        super(maxDegree);
    }

    public InternalNode(Node node) {
        super(node);
    }

}
