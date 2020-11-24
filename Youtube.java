/*package hadoop01;

public class Youtube {

	public static class Youtube{

		public String category;
		public double avg;
	
		public Youtube(String _category, double _avg){
			this.category = _category;
			this.avg = _avg;
		}

		public String getString(){
		
			return category+"|"+avg;
		}
	}

	public static class YTComparator implements Comparator<Youtube>{
	
		public int compare(Youtube x, Youtube y){
		
			//if(Double.parseDouble(x.avg) >Double.parseDouble(y.avg)) return 1;
			//if(Double.parseDouble(x.avg) < Double.parseDouble(y.avg)) return -1;
			if(x.avg > y.avg) return 1;
			if(x.avg < y.avg) return -1;
			return 0;
		}
	} 

	public static void insertYT(PriorityQueue q, String category, double avg, int topK){
	
		Youtube yt_head = (Youtube)q.peek();
	//	if(q.size() < topK || Double.parseDouble(yt_head.avg) < Double.parseDouble(avg)){
		if(q.size() < topK || yt_head.avg < avg){	
			Youtube you = new Youtube(category, avg);
			q.add(you);
			if(q.size() > topK) q.remove();
		}
	}

	public static class YTMapper extends Mapper<Object, Text, Text, DoubleWritable>{
	
	
		private Text category = new Text();
		private DoubleWritable rating = new DoubleWritable();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		
			int cnt = 1;
                        String cat = "";
                        double avg_rate = 0.0;
                        StringTokenizer itr = new StringTokenizer(value.toString(), "|");
                        while(itr.hasMoreTokens()){
                                String val = itr.nextToken().trim();
                                if(cnt == 4){
                                        cat = val;
                                }else if(cnt == 7){
                                        avg_rate = Double.parseDouble(val);
                                }

                                cnt++;
                        }
			category.set(cat);
			rating.set(avg_rate);
			context.write(category, rating);

		}
	
	}


	public static class YTReducer extends Reducer<Text, DoubleWritable, Text, Text>{
	
		private Text result = new Text();
		private Text one_key = new Text();

		public void reduce (Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
		
			double sum = 0.0;
			int cnt = 0;
			for(DoubleWritable val : values){
			
				double a = val.get();
				sum+=a;
				cnt++;
			}
			sum /= cnt;
			result.set(String.valueOf(sum));
			one_key.set(key+"|");
			context.write(one_key, result);

		}
	
	}

	public static class TopKMapper extends Mapper<Object, Text, Text, NullWritable>{
	
		private PriorityQueue<Youtube> queue;
		private Comparator<Youtube> comp = new YTComparator();
		private int topK;
		
		public void map (Object key, Text value, Context context) throws IOException, InterruptedException{
		
			StringTokenizer itr = new StringTokenizer(value.toString(),"|");
			String category = itr.nextToken().trim();
			double avg_rate = Double.parseDouble(itr.nextToken().trim());
		
			insertYT(queue, category, avg_rate, topK);
		}

		protected void setup(Context context) throws IOException, InterruptedException{
		
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK", -1);
			queue = new PriorityQueue<Youtube>(topK, comp);
		}

		protected void cleanup(Context context) throws IOException, InterruptedException{
		
			while(queue.size()!=0){
				Youtube yt = (Youtube)queue.remove();
				context.write(new Text(yt.getString()), NullWritable.get());
			}

		}
	}


	public static class TopKReducer extends Reducer<Text, NullWritable, Text, NullWritable>{
	
		private PriorityQueue<Youtube> queue;
                private Comparator<Youtube> comp = new YTComparator();
                private int topK;

                public void reduce (Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException{

                        StringTokenizer itr = new StringTokenizer(key.toString(), "|");
                        String category = itr.nextToken().trim();
                        double avg = Double.parseDouble(itr.nextToken().trim());
                        insertYT(queue, category, avg, topK);
                }

                protected void setup(Context context) throws IOException, InterruptedException{

                        Configuration conf = context.getConfiguration();
                        topK = conf.getInt("topK", -1);
                        queue = new PriorityQueue<Youtube>(topK, comp);
                }

                protected void cleanup(Context context) throws IOException, InterruptedException{

                        while(queue.size()!=0){
                                Youtube yt = (Youtube)queue.remove();
				String tt = yt.category+" "+yt.avg;
                                context.write(new Text(tt), NullWritable.get());
                        }

                }


	}


	public static void main(String[] args) throws Exception{
	
		Configuration conf = new Configuration();
		String first = "/first_phase_result";
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		int topK = Integer.parseInt(otherArgs[2]);
		if(otherArgs.length != 3){
		
			System.err.println("Usage: TopK <in> <out>");
			System.exit(3);
		}
		
		conf.setInt("topK", topK);

		Job job = new Job(conf, "YouTube");
                job.setJarByClass(Youtube.class);
                job.setMapperClass(YTMapper.class);
                job.setReducerClass(YTReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(DoubleWritable.class);
                FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
                FileOutputFormat.setOutputPath(job, new Path(first));
                FileSystem.get(job.getConfiguration()).delete(new Path(first), true);
                job.waitForCompletion(true);

		Job job2 = new Job(conf, "YouTube2");
		job2.setJarByClass(YouTube.class);
		job2.setMapperClass(TopKMapper.class);
		job2.setReducerClass(TopKReducer.class);
		//job2.setNumReduceTasks(0);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job2, new Path(first));
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]));
		FileSystem.get(job2.getConfiguration()).delete(new Path(otherArgs[1]), true);
		job2.waitForCompletion(true);
	
	}

}*/
