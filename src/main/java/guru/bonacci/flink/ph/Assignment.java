package guru.bonacci.flink.ph;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;


public class Assignment {

	/**
	 * ip_data.txt file is of following schema
		## user_id,network_name,user_IP,user_country,website, Time spent before next click
	
		 For every 10 second find out for US country
		
		a.) total number of clicks on every website in separate file
		
		b.) the website with maximum number of clicks in separate file.
		
		c.) the website with minimum number of clicks in separate file.
		
		c.) Calculate number of distinct users on every website in separate file.    
		
		d.) Calculate the average time spent on website by users.
	 */
	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Tuple6<String, String, String, String, String, Integer>> clickData = 
			env.readTextFile("file:///mnt/c/tmp/flink/ip-data.txt").map(new ParseClickData());

		DataStream<Tuple6<String, String, String, String, String, Integer>> usData = 
			clickData.filter(new FilterFunction<Tuple6<String,String,String,String,String,Integer>>() {
			
			@Override
			public boolean filter(Tuple6<String, String, String, String, String, Integer> value) throws Exception {
				return value.f3.equals("US");
			}
		});
        usData.writeAsText("file:///mnt/c/tmp/flink/us.txt", WriteMode.OVERWRITE);

		DataStream<Tuple2<String, Integer>> windowedData = 
				usData.map(new MapFunction<Tuple6<String,String,String,String,String,Integer>, Tuple2<String, Integer>>() {

					@Override
					public Tuple2<String, Integer> map(Tuple6<String, String, String, String, String, Integer> value) throws Exception {
						return Tuple2.of(value.f4, 1);
					}
					
				})
				.keyBy(0).window(TumblingProcessingTimeWindows.of(Time.seconds(10))).sum(1);

		windowedData.writeAsText("file:///mnt/c/tmp/flink/window.txt", WriteMode.OVERWRITE);
		env.execute("Product Hierarchy Demo");
	}
	
	public static class ParseClickData implements MapFunction<String, Tuple6<String, String, String, String, String, Integer>> {
		public Tuple6<String, String, String, String, String, Integer> map(String value)  {
			String[] words = value.split(",");                             
			return new Tuple6<String, String, String, String, String, Integer>
				(words[0], words[1], words[2], words[3], words[4], Integer.parseInt(words[5])); 
		}
	}

}
