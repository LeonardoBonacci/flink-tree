package p1.model;

import java.util.List;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
public class ProductHierarchyWrapper {

    public String id;    
    public JsonNode json;
    public List<String> parents;
    public JsonNode parent;
    
    @Override
    public String toString() {
        return "[" + id + " : " + json + ":" + parents + " : " + parent + "]";
    }
}