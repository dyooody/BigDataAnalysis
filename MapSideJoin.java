import java.io.*;
import java.util.*;
import java.lang.*;
import java.net.*;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class MapSideJoin{

	public static class MapSideJoinMapper extends Mapper<Object, Text, Text, Text>{
	
		Hashtable<String, String> joinMap = new Hashtable<String, String>();
		Text joinKey = new Text();
		Text joinValue = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			StringTokenizer str = new StringTokenizer(value.toString(), "|");
			String _id = str.nextToken().trim();
			String _price = str.nextToken().trim();
			String _code = str.nextToken().trim();

			String _desc = joinMap.get(_code);
			
			joinKey.set(_id);
			joinValue.set(_price+","+_desc);
			context.write(joinKey, joinValue);
		}
		
		protected void setup(Context context) throws IOException, InterruptedException{
		
			Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			BufferedReader br = new BufferedReader(new FileReader(cacheFiles[0].toString()));

			String line = br.readLine();
			while(line != null){
			
				StringTokenizer itr = new StringTokenizer(line, "|");
				String category = itr.nextToken();
				String category_name = itr.nextToken();
				joinMap.put(category, category_name);
				line = br.readLine();
			}
		}		
	}

	public static void main(String[] args) throws Exception{
	
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length != 2){
		
			System.err.println("Usage: MapSideJoin <in> <out>");
			System.exit(2);
		}
		
		Job job = new Job(conf, "MapSideJoin");
		DistributedCache.addCacheFile(new URI("input/relation_b.txt"), job.getConfiguration());
		job.setJarByClass(MapSideJoin.class);
		job.setMapperClass(MapSideJoinMapper.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete(new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true)?0:1);
	}


}
