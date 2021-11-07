package p1;

import static org.apache.flink.table.api.Expressions.$;
import static p1.functions.Utils.*;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import p1.SerDes.JsonNodeDeSchema;
import p1.SerDes.JsonNodeSerSchema;
import p1.functions.BackToJsonNode;
import p1.functions.InsertTypeFilter;
import p1.functions.ToHierarchy;
import p1.functions.ToProduct;
import p1.functions.ToProductHierarchy;
import p1.model.HierarchyWrapper;
import p1.model.ProductHierarchyWrapper;
import p1.model.ProductWrapper;

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

	
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        final DataStream<HierarchyWrapper> hierarchyData = 
        		env.addSource(new FlinkKafkaConsumer011<>(HIERARCHY_TOPIC, new JsonNodeDeSchema(), sourceProps()))
        		   .flatMap(new ToHierarchy());

        // fault tolerance trick
        final DataStream<HierarchyWrapper> faultTolerantHierarchyData = 
        		hierarchyData.keyBy(new KeySelector<HierarchyWrapper, String>() {
		    
			    	@Override
				    public String getKey(HierarchyWrapper h) throws Exception {
				        return h.hid;
				    }
				})
				.window(GlobalWindows.create())
				.trigger(CountTrigger.of(1))
				.reduce(new ReduceFunction<HierarchyWrapper>() {

					@Override
					public HierarchyWrapper reduce(HierarchyWrapper old, HierarchyWrapper _new) throws Exception {
						return _new;
					}
		});

		final IterativeStream<HierarchyWrapper> hierarchyIteration = faultTolerantHierarchyData.iterate(5000); 

		// feed data back to next iteration
		final DataStream<HierarchyWrapper> withoutPathToRoot = hierarchyIteration
				.filter(new FilterFunction<HierarchyWrapper>() {
					public boolean filter(HierarchyWrapper hierarchy) {
						return !isDoneIterating(hierarchy);
					}
				});
		hierarchyIteration.closeWith(withoutPathToRoot);

		// done iterating
		final DataStream<HierarchyWrapper> withPathToRoot = hierarchyIteration
				.filter(new FilterFunction<HierarchyWrapper>() {
					public boolean filter(HierarchyWrapper hierarchy) {
						return isDoneIterating(hierarchy);
					}
				});

		final DataStream<ProductWrapper> productData = 
				env.addSource(new FlinkKafkaConsumer011<>(PRODUCT_TOPIC, new JsonNodeDeSchema(), sourceProps()))
				   .flatMap(new ToProduct());

		 // fault tolerance trick
        final DataStream<ProductWrapper> faultTolerantProductData = 
        		productData.keyBy(new KeySelector<ProductWrapper, String>() {
		   
		        	@Override
				    public String getKey(ProductWrapper p) throws Exception {
				        return p.id;
				    }
        		})
				.window(GlobalWindows.create())
				.trigger(CountTrigger.of(1))
				.reduce(new ReduceFunction<ProductWrapper>() {
			
					@Override
					public ProductWrapper reduce(ProductWrapper old, ProductWrapper _new) throws Exception {
						return _new;
					}
		});
        
		Table hierarachyTable = tableEnv.fromDataStream(withPathToRoot);
		Table productTable = tableEnv.fromDataStream(faultTolerantProductData);

		Table productHierarchyTable = productTable.leftOuterJoin(hierarachyTable, 
					$(PARENT_ID_FIELD).isEqual($("hid"))).select(projection());

		final DataStream<ProductHierarchyWrapper> processed = 
				tableEnv.toChangelogStream(productHierarchyTable)
						.filter(new InsertTypeFilter())
						.map(new ToProductHierarchy());

		final DataStream<JsonNode> result = processed.map(new BackToJsonNode());
		
		result.addSink(new FlinkKafkaProducer011<JsonNode>(PRODUCT_HIERARCHY_TOPIC, new JsonNodeSerSchema(), sinkProps()));

		env.execute("Product Hierarchy Demo");
	}
	
	/**
	 * State logic with fault tolerance backup of global window streams
	 * @param h
	 * @return
	 */
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
}
