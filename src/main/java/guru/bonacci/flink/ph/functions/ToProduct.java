package guru.bonacci.flink.ph.functions;

import static guru.bonacci.flink.ph.ProductHierarchyJob.ID_FIELD;
import static guru.bonacci.flink.ph.ProductHierarchyJob.PARENT_IDS_FIELD;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.util.Collector;

import guru.bonacci.flink.ph.model.ProductWrapper;

public class ToProduct implements FlatMapFunction<JsonNode, ProductWrapper> {

	private static final long serialVersionUID = 1L;

	@Override
	public void flatMap(final JsonNode json, Collector<ProductWrapper> out) throws Exception {
	    final String id = (String)json.get(ID_FIELD).asText();
	    final List<String> parentIds = 
	    		StreamSupport.stream(json.get(PARENT_IDS_FIELD).spliterator(), false)
	    			.map(pid -> pid.asText())
	    			.collect(Collectors.toList());

	    parentIds.forEach(parentId -> out.collect(new ProductWrapper(id, json, parentIds, parentId)));
	}
}
