package p1;

import static org.apache.flink.table.api.Expressions.$;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectReader;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import p1.model.HierarchyWrapper;
import p1.model.ProductHierarchyWrapper;
import p1.model.ProductWrapper;

public class JoniDemo5 {

	final static Map<String, HierarchyWrapper> tree = new HashMap<>();

	// https://stackoverflow.com/questions/53324676/how-to-use-flinkkafkaconsumer-to-parse-key-separately-k-v-instead-of-t
	public static void main(String[] args) throws Exception {
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("group.id", "test");
		
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

		DataStream<HierarchyWrapper> hierarchyData = env.addSource(
				new FlinkKafkaConsumer011<>("hierarchies", new JsonNodeDeSchema(), properties))
				.flatMap(new FlatMapFunction<JsonNode, HierarchyWrapper>() {

				@Override
				public void flatMap(final JsonNode json, Collector<HierarchyWrapper> out) throws Exception {
				    final String id = (String)json.get("id").asText();
				    String parentId = null;
				    if (json.get("parentId") != null && !json.get("parentId").isNull()) {
				    	parentId = (String)json.get("parentId").asText();
				    }	
				    out.collect(new HierarchyWrapper(id, parentId, json));
				}
			});

		// TODO something with out of order messages?
		DataStream<HierarchyWrapper> global = hierarchyData.keyBy(new KeySelector<HierarchyWrapper, String>() {
		    @Override
		    public String getKey(HierarchyWrapper h) throws Exception {
		        return h.hid;
		    }
		})
		.window(GlobalWindows.create())
		.trigger(CountTrigger.of(1))
		.reduce(new ReduceFunction<HierarchyWrapper>() {
			
			@Override
			public HierarchyWrapper reduce(HierarchyWrapper value1, HierarchyWrapper value2) throws Exception {
				// TODO compare timestamps here?
				return value2;
			}
		});

		IterativeStream<HierarchyWrapper> iteration = global.iterate(5000); 
		// feed data back to next iteration
		DataStream<HierarchyWrapper> withoutPathToRoot = iteration
				.filter(new FilterFunction<HierarchyWrapper>() {
					public boolean filter(HierarchyWrapper hierarchy) {
						return !isDoneIterating(hierarchy);
					}
				});
		iteration.closeWith(withoutPathToRoot);

		// done iterating
		DataStream<HierarchyWrapper> withPathToRoot = iteration
				.filter(new FilterFunction<HierarchyWrapper>() {
					public boolean filter(HierarchyWrapper hierarchy) {
						return isDoneIterating(hierarchy);
					}
				});

		withoutPathToRoot.writeAsText("/mnt/c/tmp/flink/nopath.txt", WriteMode.OVERWRITE);
		withPathToRoot.writeAsText("/mnt/c/tmp/flink/path.txt", WriteMode.OVERWRITE);

		DataStream<ProductWrapper> productData = env.addSource(
				new FlinkKafkaConsumer011<>("products", new JsonNodeDeSchema(), properties))
				.flatMap(new FlatMapFunction<JsonNode, ProductWrapper>() {

				@Override
				public void flatMap(final JsonNode json, Collector<ProductWrapper> out) throws Exception {
				    final String id = (String)json.get("id").asText();
				    final List<String> parentIds = StreamSupport.stream(json.get("parentIds").spliterator(), false)
		    			.map(pid -> pid.asText()).collect(Collectors.toList());

				    parentIds.forEach(parentId -> out.collect(new ProductWrapper(id, json, parentIds, parentId)));
				}
			});
		
		Table hierarachyTable = tableEnv.fromDataStream(withPathToRoot);
		Table productTable = tableEnv.fromDataStream(productData);

		Table productHierarchyTable = productTable.leftOuterJoin(hierarachyTable, 
					$("parentId").isEqual($("hid")))
						.select($("id"), 
								$("pjson").as("json"),
								$("parentIds"), 
								$("hjson").as("jsonParent"));

		DataStream<Row> joinResult = tableEnv.toChangelogStream(productHierarchyTable);
		DataStream<Row> inserted = joinResult.filter(new FilterFunction<Row>() {
			
			@Override
			public boolean filter(Row row) throws Exception {
				return RowKind.INSERT.equals(row.getKind());
			}
		});

		inserted.writeAsText("/mnt/c/tmp/flink/joinresult.txt", WriteMode.OVERWRITE);

		DataStream<ProductHierarchyWrapper> processed = inserted
			.map(new MapFunction<Row, ProductHierarchyWrapper>() {

			public ProductHierarchyWrapper map(Row row) throws Exception {
				return new ProductHierarchyWrapper(
								(String)row.getField("id"), 
								(JsonNode)row.getField("json"),
								(List<String>)row.getField("parentIds"), 
								(JsonNode)row.getField("jsonParent"));
			}
		});

		DataStream<JsonNode> result = processed
				.map(new MapFunction<ProductHierarchyWrapper, JsonNode>() {

				public JsonNode map(ProductHierarchyWrapper ph) throws Exception {
					ObjectNode product = (ObjectNode)ph.json;
					product.set("parents", ph.parent);
					return product;
				}
			});
		
		
		Properties sinkProps = new Properties();
		sinkProps.setProperty("bootstrap.servers", "localhost:9092");

		result.addSink(new FlinkKafkaProducer011<JsonNode>(
						        "product-hierarchies",
						        new JsonNodeSerSchema(),
						        sinkProps));
		result.writeAsText("/mnt/c/tmp/flink/yes.txt", WriteMode.OVERWRITE);

		env.execute("Product Hierarchy Demo");
	}

	static boolean isDoneIterating(HierarchyWrapper h) {
		if (h.pId == null) {
			h.setParent(null);
			tree.put(h.hid, h);
			return true;
		}

		HierarchyWrapper parent = tree.get(h.pId);
		if (parent == null) {
			return false;
		} else {
			h.setParent(parent);
			h.hjson.set("parent", parent.hjson);
			tree.put(h.hid, h);
			return true;
		}
	}

	static class JsonNodeDeSchema implements DeserializationSchema<JsonNode> {

		private ObjectMapper mapper = new ObjectMapper();

		@Override
		public JsonNode deserialize(byte[] bytes) throws IOException {
			final ObjectReader reader = mapper.reader();
			return reader.readTree(new ByteArrayInputStream(bytes));
		}

		@Override
		public boolean isEndOfStream(JsonNode json) {
			return false;
		}

		@Override
		public TypeInformation<JsonNode> getProducedType() {
			return TypeInformation.of(new TypeHint<JsonNode>() {
			});
		}
	}
	
	static class JsonNodeSerSchema implements SerializationSchema<JsonNode> {

		private ObjectMapper mapper = new ObjectMapper();

		@Override
		public byte[] serialize(JsonNode element) {
			try {
				return mapper.writeValueAsBytes(element);
			} catch (JsonProcessingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}
		}
	}

}
