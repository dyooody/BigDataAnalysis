import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class KMeans{
	public static class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Text>{
	
		private IntWritable one_key = new IntWritable();
		private int n_centers;
		private double[] center_x;
		private double[] center_y;

		protected void setup(Context context) throws IOException, InterruptedException{
		
			Configuration conf = context.getConfiguration();
		//	n_centers = 2;
			n_centers = conf.getInt("n_centers", -1);
			center_x = new double[n_centers];
			center_y = new double[n_centers];

			for(int i=0; i<n_centers; i++){
			
				center_x[i] = conf.getDouble("center_x_"+i, 0);
				center_y[i] = conf.getDouble("center_y_"+i, 0);
			}

			//center_x = {7/3, 20/4};
			//center_y = {8/3, 19/4};
		}

		public double getDist(double x1, double y1, double x2, double y2){
		
			double dist = (x1-x2)*(x1-x2) + (y1-y2)*(y1-y2);
			return Math.sqrt(dist);
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
			StringTokenizer itr = new StringTokenizer(value.toString(),",");
			if(itr.countTokens() < 2) return;
			if(n_centers == 0) return;
			

			double _x = Double.parseDouble(itr.nextToken().trim());
			double _y = Double.parseDouble(itr.nextToken().trim());
			int cluster_idx = 0;

			int idx = 0;
			double min = getDist(_x, _y, center_x[0], center_y[0]);
			for(int i = 1; i<n_centers; i++){
			
				double dd = getDist(_x, _y, center_x[i], center_y[i]);
				if(min > dd) idx = i;  
				
			}
			
			cluster_idx = idx;

			one_key.set(cluster_idx);
			context.write(one_key, value);
		}
	}


	public static class KMeansReducer extends Reducer<IntWritable, Text, IntWritable, Text>{
	
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		
			double x_total = 0;
			double y_total = 0;
			int cnt = 0;
			Text result = new Text();
			for(Text val : values){
				cnt++;
				StringTokenizer str = new StringTokenizer(val.toString(), ",");
				x_total += Double.parseDouble(str.nextToken().trim());
				y_total += Double.parseDouble(str.nextToken().trim());
			}
			x_total /=cnt;
			y_total /=cnt;
			result.set(x_total+" "+y_total);
			context.write(key, result);
		}
	}


	public static void main(String[] args) throws Exception{
		int n_pages = 2;
		int n_centers = 2;
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		/* if(otherArgs.length != 2){
			System.err.println("Usage:PageRank <in>");
			System.exit(2);
		}*/

		initCenters(conf, n_centers);


		for(int i=0; i < n_pages; i++){
			Job job = new Job(conf, "KMeans");
			job.setJarByClass(KMeans.class);
			job.setMapperClass(KMeansMapper.class);
			job.setReducerClass(KMeansReducer.class);

			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Text.class);
			job.setInputFormatClass(TextInputFormat.class);
			
			FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
			FileSystem.get(job.getConfiguration()).delete(new Path(otherArgs[1]), true);
			
			job.waitForCompletion(true);

			updateCenters(conf);
		
		}

	}


	public static void initCenters(Configuration conf, int n_centers){
		conf.setInt("n_centers", n_centers);
		/*for(int i = 0; i < n_centers; i++){
			conf.setDouble("center_x_"+i, (Math.random()*15+1)/(double)n_centers);
			conf.setDouble("center_y_"+i, (Math.random()*15+1)/(double)n_centers);
		}*/
		conf.setDouble("center_x_0", 7/3);
		conf.setDouble("center_x_1", 20/4);
		conf.setDouble("center_y_0", 8/3);
		conf.setDouble("center_y_1", 19/4);
	}

	public static void updateCenters(Configuration conf) throws Exception{
		FileSystem dfs = FileSystem.get(conf);
		Path filenamePath = new Path("/user/bigdata/output/part-r-00000");
		FSDataInputStream in = dfs.open(filenamePath);
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));

		String line = reader.readLine();
		while(line != null){
			StringTokenizer itr = new StringTokenizer(new String(line));
			int src_id = Integer.parseInt(itr.nextToken().trim());
			double xx = Double.parseDouble(itr.nextToken().trim());
			double yy = Double.parseDouble(itr.nextToken().trim());
			conf.setDouble("center_x_"+src_id, xx);
		       	conf.setDouble("cetner_y_"+src_id, yy);	
			line = reader.readLine();
		}
	}


}
