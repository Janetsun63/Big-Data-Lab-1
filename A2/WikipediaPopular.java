import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import java.lang.*;

public class WikipediaPopular extends Configured implements Tool {

	public static class WikipediaMapper
	extends Mapper<LongWritable, Text, Text, LongWritable>{

		private final static LongWritable countw = new LongWritable(1);
		private Text word = new Text();

		// @Override
		public void map(LongWritable key, Text value, Context context
      		) throws IOException, InterruptedException {
        	String[] words = value.toString().split("\\s+");
            String date = words[0];
            String language = words[1];
            String title = words[2];
            String count = words[3];
			if ((language.equals("en")) && (!title.equals("Main_Page") && (!title.startsWith("Special:")))){
				word.set(date);
                countw.set(Long.parseLong(count));
                context.write(word, countw);
			}	
			
		}
	}


	public static class WikipediaReducer
	extends Reducer<Text,LongWritable,Text,LongWritable>{
		private LongWritable result = new LongWritable();

		// @Override
		public void reduce(Text key, Iterable<LongWritable> values,
			Context context)
			 throws IOException, InterruptedException {
		long max_count = 0;
		for (LongWritable val : values) {
			if(val.get() > max_count) {
               max_count = val.get();
		    }
        }
		result.set(max_count);
		context.write(key, result);
	}
}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WikipediaPopular(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WikipediaPopular.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(WikipediaMapper.class);
		job.setCombinerClass(WikipediaReducer.class);
		job.setReducerClass(WikipediaReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
