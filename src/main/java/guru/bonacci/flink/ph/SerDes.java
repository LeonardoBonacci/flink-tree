package guru.bonacci.flink.ph;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectReader;

@SuppressWarnings("serial")
public class SerDes {

	public static class JsonNodeDeSchema implements DeserializationSchema<JsonNode> {

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
	
	public static class JsonNodeSerSchema implements SerializationSchema<JsonNode> {

		private ObjectMapper mapper = new ObjectMapper();

		@Override
		public byte[] serialize(JsonNode element) {
			try {
				return mapper.writeValueAsBytes(element);
			} catch (JsonProcessingException e) {
				e.printStackTrace();
				return null;
			}
		}
	}


}
