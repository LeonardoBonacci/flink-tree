package guru.bonacci.flink.ph.functions;

import static guru.bonacci.flink.ph.ProductHierarchyJob.PARENTS_FIELD;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import guru.bonacci.flink.ph.model.ProductHierarchyWrapper;

public class BackToJsonNode implements MapFunction<ProductHierarchyWrapper, JsonNode> {

	private static final long serialVersionUID = 1L;

	@Override
	public JsonNode map(ProductHierarchyWrapper ph) throws Exception {
		ObjectNode product = (ObjectNode)ph.json;
		product.set(PARENTS_FIELD, ph.parent);
		return product;
	}
}
