package guru.bonacci.flink.ph.functions;

import static guru.bonacci.flink.ph.ProductHierarchyJob.ID_FIELD;
import static guru.bonacci.flink.ph.ProductHierarchyJob.JSON_FIELD;
import static guru.bonacci.flink.ph.ProductHierarchyJob.JSON_PARENT_FIELD;
import static guru.bonacci.flink.ph.ProductHierarchyJob.PARENT_IDS_FIELD;
import static org.apache.flink.table.api.Expressions.$;

import java.util.Properties;

import org.apache.flink.table.expressions.Expression;

public class Utils {

	public static Expression[] projection() {
		Expression selectors[] = new Expression[] { 
			$(ID_FIELD), 
			$("pjson").as(JSON_FIELD),
			$(PARENT_IDS_FIELD), 
			$("hjson").as(JSON_PARENT_FIELD)
		};
		return selectors;
	}

	public static Properties sourceProps() {
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("group.id", "test");
		return properties;
	}

	public static Properties sinkProps() {
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		return properties;
	}
}
