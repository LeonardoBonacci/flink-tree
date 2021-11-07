package guru.bonacci.flink.ph.model;

import java.util.List;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
public class ProductWrapper {

    public String id;    
    @JsonIgnore public JsonNode pjson;    
    public List<String> parentIds;
    public String parentId;

    @Override
    public String toString() {
        return "[" + id + " : " + parentId + "] <> " + pjson;
    }
}