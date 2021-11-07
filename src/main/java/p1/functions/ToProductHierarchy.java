package p1.functions;

import static p1.ProductHierarchyJob.ID_FIELD;
import static p1.ProductHierarchyJob.JSON_FIELD;
import static p1.ProductHierarchyJob.JSON_PARENT_FIELD;
import static p1.ProductHierarchyJob.PARENT_IDS_FIELD;

import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.types.Row;

import p1.model.ProductHierarchyWrapper;

public class ToProductHierarchy implements MapFunction<Row, ProductHierarchyWrapper> {

	private static final long serialVersionUID = 1L;

	@SuppressWarnings("unchecked")
	@Override
	public ProductHierarchyWrapper map(Row row) throws Exception {
		return new ProductHierarchyWrapper(
						(String)row.getField(ID_FIELD), 
						(JsonNode)row.getField(JSON_FIELD),
						(List<String>)row.getField(PARENT_IDS_FIELD), 
						(JsonNode)row.getField(JSON_PARENT_FIELD));
	}
}
