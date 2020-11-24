import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;



public class MatrixMul_Sec {
	public static class MatString implements WritableComparable{

		String orderkey = new String();
		String sortingkey = new String();
		
		public MatString(){}
		
		public MatString(String _val, String _x){
			this.orderkey = _val;
			this.sortingkey = _x;
		}
		
		public void readFields(DataInput in)throws IOException{
			orderkey = in.readUTF();
			sortingkey = in.readUTF();
		}
		
		public void write(DataOutput out) throws IOException{
			out.writeUTF(orderkey);
			out.writeUTF(sortingkey);
		}
		
		public int compareTo(Object o1){
			MatString o = (MatString) o1;

			int ret = sortingkey.compareTo(o.sortingkey);
			if(ret!=0) return ret;
			return orderkey.compareTo(o.orderkey);
		}
		
		public String getString(){
			
			return orderkey+"|"+sortingkey;
		}
		
	}
	
	public static class CompositeKeyComparator extends WritableComparator{
		protected CompositeKeyComparator(){
			super(MatString.class, true);
		}
		public int compare(WritableComparable w1, WritableComparable w2){
			MatString k1 = (MatString)w1;
			MatString k2 = (MatString)w2;
			
			int result = k1.orderkey.compareTo(k2.orderkey);
			if(0 == result){
				result = k1.sortingkey.compareTo(k2.sortingkey);
			}
			
			return result;
		}
	}
	
	public static class FirstPartitioner extends Partitioner<MatString, Text>{
		public int getPartition(MatString key, Text value, int numPartition){
			return key.orderkey.hashCode()%numPartition;	
		}
	}

	public static class FirstGroupingComparator extends WritableComparator{
	
		protected FirstGroupingComparator(){
			super(MatString.class, true);
		}
		public int compare(WritableComparable w1, WritableComparable w2){
			MatString k1 = (MatString) w1;
			MatString k2 = (MatString) w2;

			return k1.orderkey.compareTo(k2.orderkey);
		}
	}
	
	
	public static class MulSecMap extends Mapper<Object, Text, MatString, Text>{
		private Text outputValue = new Text();
		private Text www = new Text();		

		private int m;
		private int k;
		private int n;
		
		boolean fileA = true;
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			MatString outputkey = new MatString();
			
			StringTokenizer str = new StringTokenizer(value.toString());
			String row_id = str.nextToken().trim();
			String col_id = str.nextToken().trim();
			String mat_val = str.nextToken().trim();
			
			if(fileA){
				for(int i =0; i < n; i++){
					outputkey.orderkey = row_id+","+String.valueOf(i);
					outputkey.sortingkey = col_id;
					String aa = mat_val;
					outputValue.set(aa);
					context.write(outputkey, outputValue);
				}
			}else{
				for(int j = 0; j < m; j++){
					outputkey.orderkey = String.valueOf(j)+","+col_id;
					outputkey.sortingkey = row_id;
					String bb = mat_val;
					outputValue.set(bb);
					context.write(outputkey, outputValue);
					
				}
			}
		}
		
		public void setup(Context context) throws IOException, InterruptedException{
			Configuration conf = context.getConfiguration();
			
			m = conf.getInt("m", -1);
			k = conf.getInt("k", -1);
			n = conf.getInt("n", -1);
			
			String filename = ((FileSplit)context.getInputSplit()).getPath().getName();
			
			if(filename.indexOf("matrix.txt")!= -1) fileA = true;
			else fileA = false;
		}
	}
	
	public static class MulSecReduce extends Reducer<MatString, Text, Text, IntWritable>{
		private IntWritable result = new IntWritable();
		private Text reskey = new Text();
		public void reduce(MatString key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

			int sum = 0;
			int mul = 1;

			StringTokenizer itr = new StringTokenizer(key.getString(), "|");
			String first = itr.nextToken().trim();
			int second = Integer.parseInt(itr.nextToken().trim());
			
			int c = 0;

			for(Text val : values){
				c++;
				StringTokenizer str = new StringTokenizer(val.toString());
				int hh = Integer.parseInt(str.nextToken().trim());
				if(c%2==1){
					mul = hh;
				}else if(c%2==0){
					mul*= hh;
					sum+=mul;
				}	
			}

			reskey.set(first);
			result.set(sum);
			context.write(reskey, result);	
			
		}
		
	}

	public static void main(String[] args) throws Exception{

		int m_value = 2;
		int k_value = 2;
		int n_value = 2;
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		conf.setInt("m", m_value);
		conf.setInt("k", k_value);
		conf.setInt("n", n_value);
		
		Job job = new Job(conf, "MatrixMul_Sec");
                job.setJarByClass(MatrixMul_Sec.class);
                job.setMapperClass(MulSecMap.class);
                job.setReducerClass(MulSecReduce.class);

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);

                job.setMapOutputKeyClass(MatString.class);
                job.setMapOutputValueClass(Text.class);

                job.setPartitionerClass(FirstPartitioner.class);
                job.setGroupingComparatorClass(FirstGroupingComparator.class);
                job.setSortComparatorClass(CompositeKeyComparator.class);

                FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
                FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
                FileSystem.get(job.getConfiguration()).delete(new Path(otherArgs[1]), true);
                job.waitForCompletion(true);
		

	}

}
