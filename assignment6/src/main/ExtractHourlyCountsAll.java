import java.io.IOException;
import java.util.Calendar;
import java.util.Iterator;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class ExtractHourlyCountsAll extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(ExtractHourlyCountsAll.class);

	// Mapper: emits (token, 1) for every word occurrence.
	private static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		// Reuse objects to save overhead of object creation.
		private final static IntWritable ONE = new IntWritable(1);
		private final static Text DATE = new Text();
		private final static SimpleDateFormat dateForm = new SimpleDateFormat();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] line = value.toString().split("\t");
			if (line.length < 3) return;
			
			String hourString = line[1];
			dateForm.applyPattern("EEE MMM dd HH:mm:ss ZZZZZ yyyy");
			try {
				dateForm.parse(hourString);
			} catch (java.text.ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (dateForm.getCalendar().get(Calendar.DAY_OF_YEAR) >= 23 &&
					dateForm.getCalendar().get(Calendar.DAY_OF_YEAR) <= 39 &&
					dateForm.getCalendar().get(Calendar.YEAR)== 2011) {
				dateForm.applyPattern("M/dd HH");
				DATE.set(dateForm.format(dateForm.getCalendar().getTime()));
				context.write(DATE, ONE);
			}
		}
	}

	// Reducer: sums up all the counts.
	private static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		// Reuse objects.
		private final static IntWritable SUM = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			// Sum up values.
			Iterator<IntWritable> iter = values.iterator();
			int sum = 0;
			while (iter.hasNext()) {
				sum += iter.next().get();
			}
			SUM.set(sum);
			context.write(key, SUM);
		}
	}

	/**
	 * Creates an instance of this tool.
	 */
	public ExtractHourlyCountsAll() {}

	/**
	 * Runs this tool.
	 */
	@SuppressWarnings({ "static-access" })
	public int run(String[] args) throws Exception {

		String inputPath = "data/tweets.txt";
		String outputPath = "qiwang321-all";
		int reduceTasks = 1;

				LOG.info("Tool: " + ExtractHourlyCountsAll.class.getSimpleName());
				LOG.info(" - input path: " + inputPath);
				LOG.info(" - output path: " + outputPath);
				LOG.info(" - number of reducers: " + reduceTasks);

				Configuration conf = getConf();
				Job job = Job.getInstance(conf);
				job.setJobName(ExtractHourlyCountsAll.class.getSimpleName());
				job.setJarByClass(ExtractHourlyCountsAll.class);

				job.setNumReduceTasks(reduceTasks);

				FileInputFormat.setInputPaths(job, new Path(inputPath));
				FileOutputFormat.setOutputPath(job, new Path(outputPath));

				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(IntWritable.class);

				job.setMapperClass(MyMapper.class);
				job.setCombinerClass(MyReducer.class);
				job.setReducerClass(MyReducer.class);

				// Delete the output directory if it exists already.
				Path outputDir = new Path(outputPath);
				FileSystem.get(conf).delete(outputDir, true);

				long startTime = System.currentTimeMillis();
				job.waitForCompletion(true);
				LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

				return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new ExtractHourlyCountsAll(), args);
	}
}