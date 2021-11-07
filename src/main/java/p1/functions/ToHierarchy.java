package p1.functions;

import static p1.ProductHierarchyJob.ID_FIELD;
import static p1.ProductHierarchyJob.PARENT_ID_FIELD;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.util.Collector;

import p1.model.HierarchyWrapper;

public class ToHierarchy implements FlatMapFunction<JsonNode, HierarchyWrapper> {

	private static final long serialVersionUID = 1L;

	@Override
	public void flatMap(final JsonNode json, Collector<HierarchyWrapper> out) throws Exception {
		String parentId = null;
		final String id = (String) json.get(ID_FIELD).asText();
		final JsonNode parentIdNode = json.get(PARENT_ID_FIELD);
		if (parentIdNode != null && !parentIdNode.isNull()) {
			parentId = parentIdNode.asText();
		}
		out.collect(new HierarchyWrapper(id, parentId, json));
	}
}
