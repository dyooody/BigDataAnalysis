package hadoop01;

import java.io.*;
import java.util.*;
import java.util.regex.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;


public class IMDB {

	public static class Movie{
		
		public String name;
		public String rate;

		public Movie(String _name, String _rate){
			this.name = _name;
			this.rate = _rate;
		}

		public String getString(){
			return name+"|"+rate;
		}
	}

	public static class MovieComparator implements Comparator<Movie>{
		public int compare(Movie x, Movie y){
	
			if(Double.parseDouble(x.rate) > Double.parseDouble(y.rate)) return 1;
			if(Double.parseDouble(x.rate) < Double.parseDouble(y.rate)) return -1;
			return 0;
		}
	}


	public static void insertMovie(PriorityQueue q, String name, String rate, int topK){
	
		Movie m_head = (Movie)q.peek();
		if(q.size() < topK || Double.parseDouble(m_head.rate) < Double.parseDouble(rate)){
			Movie m = new Movie(name, rate);
			q.add(m);
			if(q.size() > topK) q.remove();
		}
	}

	public static class DoubleString implements WritableComparable{

                String joinKey = new String();
                String tableName = new String();

                public DoubleString(){}
                public DoubleString(String _joinKey, String _tableName){

                        joinKey = _joinKey;
                        tableName = _tableName;
                }

                public void readFields(DataInput in) throws IOException{
                        joinKey = in.readUTF();
                        tableName = in.readUTF();
                }

                public void write(DataOutput out) throws IOException{
                        out.writeUTF(joinKey);
                        out.writeUTF(tableName);
                }

                public int compareTo(Object o1){
                        DoubleString o = (DoubleString) o1;
                        int ret = joinKey.compareTo(o.joinKey);
                        if(ret!=0) return ret;
                        return tableName.compareTo(o.tableName);
                }

                public String toString(){return joinKey+" "+tableName;}
        }

	public static class CompositeKeyComparator extends WritableComparator{
		protected CompositeKeyComparator(){
			super(DoubleString.class, true);
		}
		public int compare(WritableComparable w1, WritableComparable w2){
			DoubleString k1 = (DoubleString) w1;
			DoubleString k2 = (DoubleString) w2;

			int result = k1.joinKey.compareTo(k2.joinKey);
			if(0 == result){
				result = k1.tableName.compareTo(k2.tableName);
			}
			return result;
		}
	}


	public static class FirstPartitioner extends Partitioner<DoubleString, Text>{
		public int getPartition(DoubleString key, Text value, int numPartition){
			return key.joinKey.hashCode()%numPartition;
		}
	}

	public static class FirstGroupingComparator extends WritableComparator{
	
		protected FirstGroupingComparator(){
			super(DoubleString.class, true);
		}
		public int compare(WritableComparable w1, WritableComparable w2){
			DoubleString k1 = (DoubleString) w1;
			DoubleString k2 = (DoubleString) w2;

			return k1.joinKey.compareTo(k2.joinKey);
		}
	}

	public static class IMDBMapper extends Mapper<Object, Text, DoubleString, Text>{
		boolean fileA = true;
		public void map(Object Key, Text value, Context context) throws IOException, InterruptedException{
		
			String delim = "#";
			String changed = value.toString().replace("::", delim);
			StringTokenizer itr = new StringTokenizer(changed, delim);
			//StringTokenizer itr = new StringTokenizer(value.toString(), "::");
			DoubleString outputKey = new DoubleString();
			Text outputValue = new Text();
			String o_value = "";
	

			if(fileA){
				Pattern p = Pattern.compile("(::)");
				String split[] = value.toString().split(p.pattern);
				String _id = split[0];
				String _name = split[1];
				String _genre = split[2];
				
				String _id = itr.nextToken().trim();
				String _name = itr.nextToken().trim();
				String _genre = itr.nextToken().trim();
				o_value = new StringBuilder().append(_name+"*"+_genre).toString();	
				outputKey.joinKey = _id;
				outputKey.tableName = "A";
			}
			else{
				String _uid = itr.nextToken().trim();
				String _id = itr.nextToken().trim();
				outputKey.joinKey = _id;
				outputKey.tableName = "B";
				String _rate = itr.nextToken().trim();
				o_value = _rate;
				String _null = itr.nextToken().trim();
			}
			outputValue.set(o_value);
			context.write(outputKey, outputValue);

		}

		protected void setup(Context context) throws IOException, InterruptedException{
		
			String filename = ((FileSplit)context.getInputSplit()).getPath().getName();

			if(filename.indexOf("movies")!= -1) fileA=true;
			else fileA = false;
		}
	}


	public static class IMDBReducer extends Reducer<DoubleString, Text, Text, Text>{

		
		public void reduce(DoubleString key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			Text reduce_key = new Text();
			Text reduce_result = new Text();
			String movie = "";
			double rating = 0;
			int cnt = 0;

			for(Text val : values){
				if(movie.length() == 0){
					StringTokenizer str = new StringTokenizer(val.toString(), "*");
					String name = str.nextToken();
					String genre = str.nextToken();
					if(genre.indexOf("Fantasy") != -1)
                               			movie = name;
					else
						movie = "null";
				
				}else{
					cnt++;
					String rate = val.toString();
					rating += Double.parseDouble(rate);
				}

			}

			rating /= (double)cnt;
			if(!movie.equals("null")){
				reduce_key.set(movie+"|");
                String result = String.valueOf(rating);
                reduce_result.set(result);
                context.write(reduce_key, reduce_result);}

	
		}
	
	}


	public static class TopKMapper extends Mapper<Object, Text, Text, NullWritable>{
	
		private PriorityQueue<Movie> queue;
		private Comparator<Movie> comp = new MovieComparator();
		private int topK;

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		
			StringTokenizer itr = new StringTokenizer(value.toString(),"|");
			String m_name = itr.nextToken().trim();
			String m_rate = itr.nextToken().trim();
			insertMovie(queue, m_name, m_rate, topK);
		}

		protected void setup(Context context) throws IOException, InterruptedException{
		
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK", -1);
			queue = new PriorityQueue<Movie>(topK, comp);
		}

		protected void cleanup(Context context) throws IOException, InterruptedException{
		
			while(queue.size() != 0){
			
				Movie mov = (Movie) queue.remove();
				context.write(new Text(mov.getString()), NullWritable.get());
			}
		}
	}


	public static class TopKReducer extends Reducer<Text, NullWritable, Text, NullWritable>{
	
		private PriorityQueue<Movie> queue;
                private Comparator<Movie> comp = new MovieComparator();
                private int topK;

                public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException{

                        StringTokenizer itr = new StringTokenizer(key.toString(),"|");
                        String m_name = itr.nextToken().trim();
                        String m_rate = itr.nextToken().trim();
                        insertMovie(queue, m_name, m_rate, topK);
                }

                protected void setup(Context context) throws IOException, InterruptedException{

                        Configuration conf = context.getConfiguration();
                        topK = conf.getInt("topK", -1);
                        queue = new PriorityQueue<Movie>(topK, comp);
                }

                protected void cleanup(Context context) throws IOException, InterruptedException{

                        while(queue.size() != 0){

                                Movie mov = (Movie) queue.remove();
				String dd = mov.name+" "+mov.rate;
			       	context.write(new Text(dd), NullWritable.get());
                        }
                }

	
	}

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		
		String first_phase_result = "/first_phase_result";
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length != 3){
			System.err.println("Usage: ReduceSideJoin2 <in> <out>");
			System.exit(3);
		}
		int topK = Integer.parseInt(otherArgs[2]);
		conf.setInt("topK", topK);

		Job job = new Job(conf, "IMDB");
		job.setJarByClass(IMDB.class);
		job.setMapperClass(IMDBMapper.class);
		job.setReducerClass(IMDBReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapOutputKeyClass(DoubleString.class);
		job.setMapOutputValueClass(Text.class);

		job.setPartitionerClass(FirstPartitioner.class);
		job.setGroupingComparatorClass(FirstGroupingComparator.class);
		job.setSortComparatorClass(CompositeKeyComparator.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(first_phase_result));
		FileSystem.get(job.getConfiguration()).delete(new Path(first_phase_result), true);
		job.waitForCompletion(true);


		Job job2 = new Job(conf, "IMDB2");
		job2.setJarByClass(IMDB.class);
		job2.setMapperClass(TopKMapper.class);
		job2.setReducerClass(TopKReducer.class);
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.addInputPath(job2, new Path(first_phase_result));
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]));
		FileSystem.get(job2.getConfiguration()).delete(new Path(otherArgs[1]), true);
		job2.waitForCompletion(true);

	}


}
