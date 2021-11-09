package guru.bonacci.flink.ph;

import static guru.bonacci.flink.ph.functions.Utils.projection;
import static guru.bonacci.flink.ph.functions.Utils.sinkProps;
import static guru.bonacci.flink.ph.functions.Utils.sourceProps;
import static org.apache.flink.table.api.Expressions.$;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import guru.bonacci.flink.ph.SerDes.JsonNodeDeSchema;
import guru.bonacci.flink.ph.SerDes.JsonNodeSerSchema;
import guru.bonacci.flink.ph.functions.BackToJsonNode;
import guru.bonacci.flink.ph.functions.InsertTypeFilter;
import guru.bonacci.flink.ph.functions.Snapshot;
import guru.bonacci.flink.ph.functions.ToHierarchy;
import guru.bonacci.flink.ph.functions.ToProduct;
import guru.bonacci.flink.ph.functions.ToProductHierarchy;
import guru.bonacci.flink.ph.model.HierarchyWrapper;
import guru.bonacci.flink.ph.model.ProductHierarchyWrapper;
import guru.bonacci.flink.ph.model.ProductWrapper;

@SuppressWarnings("serial")
public class ProductHierarchyJob {

	public final static String HIERARCHY_TOPIC = "hierarchies";
	public final static String PRODUCT_TOPIC = "products";
	public final static String PRODUCT_HIERARCHY_TOPIC = "product-hierarchies";

	public final static String ID_FIELD = "id";
	public final static String PARENTS_FIELD = "parents";
	public final static String PARENT_ID_FIELD = "parentId";
	public final static String PARENT_IDS_FIELD = "parentIds";
	public final static String JSON_FIELD = "json";
	public final static String JSON_PARENT_FIELD = "jsonParent";
	
	final static Map<String, HierarchyWrapper> tree = new HashMap<>();

	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE, true);
		env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
		env.getCheckpointConfig().enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
		
		final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        final DataStream<HierarchyWrapper> hierarchyData = 
        		env.addSource(new FlinkKafkaConsumer011<>(HIERARCHY_TOPIC, new JsonNodeDeSchema(), sourceProps()))
        		   .flatMap(new ToHierarchy());

        final DataStream<HierarchyWrapper> faultTolerantHierarchyData = 
        		hierarchyData.keyBy(HierarchyWrapper::getHid).map(new Snapshot());

		final IterativeStream<HierarchyWrapper> iterator = faultTolerantHierarchyData.iterate(); 

		final DataStream<HierarchyWrapper> iteration = 
				iterator.map(new MapFunction<HierarchyWrapper, HierarchyWrapper>()  {

			public HierarchyWrapper map(HierarchyWrapper h) {
					if (h.pId == null) {
						h.setParent(null);
						tree.put(h.hid, h);
						return h;
					} 

					HierarchyWrapper parent = tree.get(h.pId);
					if (parent != null) {
						h.setParent(parent);
						h.hjson.set("parent", parent.hjson);
						tree.put(h.hid, h);
					}
					return h;
				}
		});   

		// keep iterating
		final DataStream<HierarchyWrapper> withoutPathToRoot = iteration.filter(new FilterFunction<HierarchyWrapper>() {
			public boolean filter(HierarchyWrapper h) {
				return !tree.containsKey(h.hid);
			}
		}); 
		// feed data back to next iteration
		iterator.closeWith(withoutPathToRoot);

		// leave iteration
		final DataStream<HierarchyWrapper> withPathToRoot = iteration.filter(new FilterFunction<HierarchyWrapper>() {
			public boolean filter(HierarchyWrapper h) {
				return tree.containsKey(h.hid);
			}
		});

		withPathToRoot.print("hierarchy-sink");

		final DataStream<ProductWrapper> productData = 
				env.addSource(new FlinkKafkaConsumer011<>(PRODUCT_TOPIC, new JsonNodeDeSchema(), sourceProps()))
				   .flatMap(new ToProduct());

		final Table hierarachyTable = tableEnv.fromDataStream(withPathToRoot);
		final Table productTable = tableEnv.fromDataStream(productData);

		final Table productHierarchyTable = 
				productTable.leftOuterJoin(hierarachyTable, $(PARENT_ID_FIELD).isEqual($("hid")))
						.select(projection());

		final DataStream<ProductHierarchyWrapper> processed = 
				tableEnv.toChangelogStream(productHierarchyTable)
						.filter(new InsertTypeFilter())
						.map(new ToProductHierarchy());

		final DataStream<JsonNode> result = processed.map(new BackToJsonNode());
		
		result.addSink(new FlinkKafkaProducer011<JsonNode>(PRODUCT_HIERARCHY_TOPIC, new JsonNodeSerSchema(), sinkProps()));

		env.execute("Product Hierarchy Demo");
	}
}
