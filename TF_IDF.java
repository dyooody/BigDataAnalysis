import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class TF_IDF{

	public static class TF_IDFMap1 extends Mapper<LongWritable, Text, Text, IntWritable>{
	
		private IntWritable val1 = new IntWritable();
		private Text key1 = new Text();
		private String doc_name;

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
			StringTokenizer str = new StringTokenizer(value.toString());
			while(str.hasMoreTokens()){
			
				String a = str.nextToken().toLowerCase();
				key1.set(a+" "+doc_name);
				val1.set(1);
				context.write(key1, val1);
			}
		
		}

		public void setup(Context context) throws IOException, InterruptedException{
		
			doc_name = ((FileSplit)context.getInputSplit()).getPath().getName();

		}
	}


	public static class TF_IDFReduce1 extends Reducer<Text, IntWritable, Text, Text>{
	
		private Text result = new Text();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
		
			int sum = 0;
			for(IntWritable val : values){
			
				sum+= val.get();
			}
			String res = String.valueOf(sum);
			result.set(res);
			context.write(key, result);
		}
	
	}


	public static class TF_IDFMap2 extends Mapper<Object, Text, Text, Text>{
	
		private Text key2 = new Text();
		private Text val2 = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		
			StringTokenizer str = new StringTokenizer(value.toString());
			String word = str.nextToken().trim();
			String doc_name = str.nextToken().trim();
			String a = str.nextToken().trim();
			key2.set(doc_name);
			val2.set(word+","+a);

			context.write(key2, val2);
		}
	}


	public static class TF_IDFReduce2 extends Reducer<Text, Text, Text, Text>{
	
		private Text result = new Text();
		private Text keykey = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			
			ArrayList<String> buffer = new ArrayList<String>();
			int sum = 0;
			for(Text val : values){
			
				StringTokenizer itr = new StringTokenizer(val.toString(), ",");
				String word = itr.nextToken().trim();
				int n = Integer.parseInt(itr.nextToken().trim());
				sum+=n;
				String ss = word+" "+key.toString();
				buffer.add(ss+" "+n);
				
			}

			for(int i = 0; i < buffer.size(); i++){
			
				String st = buffer.get(i);
				StringTokenizer itr = new StringTokenizer(st);
				String ww = itr.nextToken().trim();
				String docu = itr.nextToken().trim();
				String nn = itr.nextToken().trim();
				keykey.set(ww+" "+docu);
				result.set(nn+" "+sum);
				context.write(keykey, result);
			}
		
		}
	
	}


	public static class TF_IDFMap3 extends Mapper<Object, Text, Text, Text>{
	
		private Text key3 = new Text();
		private Text val3 = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		
			StringTokenizer str = new StringTokenizer(value.toString());	
			String word = str.nextToken().trim();
			String docu = str.nextToken().trim();
			int n = Integer.parseInt(str.nextToken().trim());
			int nn = Integer.parseInt(str.nextToken().trim());
			
			key3.set(word);
			String aa = docu+"|"+n+"|"+nn+"|"+1;
			val3.set(aa);	
			
			context.write(key3, val3);
		}
	}

	public static class TF_IDFReduce3 extends Reducer<Text, Text, Text, Text>{
	
		private Text keey = new Text();
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		
			ArrayList<String> buffer = new ArrayList<String>();
			int sum = 0;
			for(Text val : values){
			
				StringTokenizer str = new StringTokenizer(val.toString(), "|");
				String docu = str.nextToken().trim();
				int n = Integer.parseInt(str.nextToken().trim());
				int nn = Integer.parseInt(str.nextToken().trim()); 
				int one = Integer.parseInt(str.nextToken().trim());
				
				String ss = key+" "+docu;
				String gg = n+" "+nn;
				
				buffer.add(ss+" "+gg);
				
				sum+=one;
			
			}

			for(int i = 0 ; i < buffer.size(); i++){
			
				String st = buffer.get(i);
				StringTokenizer itr = new StringTokenizer(st);
				String word = itr.nextToken().trim();
				String dd = itr.nextToken().trim();
				int n = Integer.parseInt(itr.nextToken().trim());
				int N = Integer.parseInt(itr.nextToken().trim());
				String aa = word+" "+dd;
				keey.set(aa);
				String bb = n+" "+N+" "+sum;
				result.set(bb);
				context.write(keey, result);
			
			}
		
		}
	
	}


	public static class TF_IDFMap4 extends Mapper<Object, Text, Text, Text>{
	
		private Text key4 = new Text();
		private Text val4 = new Text();
		double doc_num;
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		
			StringTokenizer str = new StringTokenizer(value.toString());
			String word = str.nextToken().trim();
			String docu = str.nextToken().trim();
			double n = Double.parseDouble(str.nextToken().trim());
			double N = Integer.parseInt(str.nextToken().trim());
			double m = Integer.parseInt(str.nextToken().trim());
			
			//double dname = (double)doc_name;
			double aaa = doc_num/m;
			double bbb = n/N;
						
			String ccc = word+","+docu;
			
			double tfidf = bbb*(Math.log(aaa));	
			String ddd = String.valueOf(tfidf);
			val4.set(ddd);

			key4.set(ccc);
				
			context.write(key4,val4); 
			
		}


		public void setup(Context context) throws IOException, InterruptedException{

                        Configuration conf = context.getConfiguration();
                        doc_num = conf.getDouble("NumOfDocs", -1.0);
                        
                }

	}



	public static void main(String[] args) throws Exception{
	
		Configuration conf = new Configuration();
	
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	
		double NumOfDocs = 4.0;
		conf.setDouble("NumOfDocs", NumOfDocs);

		String first_result = "/first_phase_result";
		String second_result = "/second_phase_result";
		String third_result = "/third_phase_result";

		Job job1 = new Job(conf, "TF_IDF1");
		job1.setJarByClass(TF_IDF.class);	
		job1.setMapperClass(TF_IDFMap1.class);
		job1.setReducerClass(TF_IDFReduce1.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job1, new Path(first_result));
		FileSystem.get(job1.getConfiguration()).delete(new Path(first_result), true);
		job1.waitForCompletion(true);



		Job job2 = new Job(conf, "TF_IDF2");
        job2.setJarByClass(TF_IDF.class);
        job2.setMapperClass(TF_IDFMap2.class);
        job2.setReducerClass(TF_IDFReduce2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(first_result));
        FileOutputFormat.setOutputPath(job2, new Path(second_result));
        FileSystem.get(job2.getConfiguration()).delete(new Path(second_result), true);
        job2.waitForCompletion(true);


		Job job3 = new Job(conf, "TF_IDF3");
		job3.setJarByClass(TF_IDF.class);
		job3.setMapperClass(TF_IDFMap3.class);
		job3.setReducerClass(TF_IDFReduce3.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job3, new Path(second_result));
		FileOutputFormat.setOutputPath(job3, new Path(third_result));
		FileSystem.get(job3.getConfiguration()).delete(new Path(third_result), true);
		job3.waitForCompletion(true);


		Job job4 = new Job(conf, "TF_IDF4");
        job4.setJarByClass(TF_IDF.class);
        job4.setMapperClass(TF_IDFMap4.class);
        //job4.setReducerClass(TF_IDFReduce1.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job4, new Path(third_result));
        FileOutputFormat.setOutputPath(job4, new Path(otherArgs[1]));
        FileSystem.get(job4.getConfiguration()).delete(new Path(otherArgs[1]), true);
        job4.waitForCompletion(true);


	
	}



}
