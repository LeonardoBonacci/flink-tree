package p1.model;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import lombok.AllArgsConstructor;

@AllArgsConstructor()
public class HierarchyWrapper {

    public String hid;    
    @JsonIgnore public ObjectNode hjson;
    public String pId;
    public HierarchyWrapper parent;
    
    public HierarchyWrapper() {}

    public HierarchyWrapper(String id, String parentId, JsonNode json) {
        this.hid = id;
        this.pId = parentId;
        this.hjson = (ObjectNode)json;
    }

    public void setParent(HierarchyWrapper p) {
    	this.parent = p;
    }

    @Override
    public String toString() {
    	return "[" + hid + " : " + pId + " : " + parent + "] <> " + hjson;
    }
}