/*
 * Cloud9: A Hadoop toolkit for working with big data
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */


import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import cern.colt.Arrays;
import edu.umd.cloud9.io.pair.PairOfStrings;

/**
 * <p>
 * Implementation of the "pairs" algorithm for computing co-occurrence matrices from a large text
 * collection. This algorithm is described in Chapter 3 of "Data-Intensive Text Processing with 
 * MapReduce" by Lin &amp; Dyer, as well as the following paper:
 * </p>
 *
 * <blockquote>Jimmy Lin. <b>Scalable Language Processing Algorithms for the Masses: A Case Study in
 * Computing Word Co-occurrence Matrices with MapReduce.</b> <i>Proceedings of the 2008 Conference
 * on Empirical Methods in Natural Language Processing (EMNLP 2008)</i>, pages 419-428.</blockquote>
 *
 * @author Jimmy Lin
 */
public class PairsPMI extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(PairsPMI.class);
  
  private static long numRecords; // counter

  // Stage 1 Mapper
  private static class MyMapper extends Mapper<LongWritable, Text, PairOfStrings, IntWritable> {
  	private static final PairOfStrings PAIR = new PairOfStrings();
    private static final IntWritable ONE = new IntWritable(1);
    
    @Override
    public void map(LongWritable key, Text line, Context context)
        throws IOException, InterruptedException {
      String text = line.toString();

      String[] terms = text.split("\\s+");
      
      //eliminate duplication
      for (int i = 0; i < terms.length; i++) {
    	  for (int j = i+1; j < terms.length; j++)
    	  {
    		  if (terms[i].equals(terms[j]))
    			  terms[j] = "\0";
    	  }
      }

      for (int i = 0; i < terms.length; i++) {

        //skip empty tokens
        if (terms[i].length() == 0 || terms[i].charAt(0) == '\0')
          continue;
        
    		PAIR.set(terms[i], "\1");
    		context.write(PAIR, ONE); //emit for marginal count

        for (int j = 0; j < terms.length; j++) {
        	
          // skip empty tokens
          if (i == j || terms[j].length() == 0 || terms[j].charAt(0) == '\0')
          	continue;
          
       		PAIR.set(terms[i], terms[j]);
       		context.write(PAIR, ONE); //emit the count of pairs
   	
        }    	
      }
    }
  }

  //Stage 1 Combiner
  private static class MyCombiner extends
     Reducer<PairOfStrings, IntWritable, PairOfStrings, IntWritable> {
   private final static IntWritable VAL = new IntWritable();
   
   @Override
   public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
       throws IOException, InterruptedException {
   	
     Iterator<IntWritable> iter = values.iterator();
     int sum = 0;
     
     while (iter.hasNext()) {
       sum += iter.next().get();
     }
     
     VAL.set(sum);
     context.write(key, VAL); 

   }
 }
  
  
  // Stage 1 Reducer
  private static class MyReducer extends
      Reducer<PairOfStrings, IntWritable, PairOfStrings, FloatWritable> {
    private final static FloatWritable VAL = new FloatWritable();
    private static int marginal;
    
    @Override
    public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
    	
      Iterator<IntWritable> iter = values.iterator();
      int sum = 0;
      
      while (iter.hasNext()) {
        sum += iter.next().get();
      }
      
      if (sum < 10) 
      	return;

      if (key.getRightElement().equals("\1")) {        	
      	marginal = sum;
      	VAL.set((float)marginal);
      	key.set(key.getRightElement(), key.getLeftElement());
      }

      else  {
      	VAL.set((float)sum / marginal);
      }
      context.write(key, VAL); 

    }
  }

  
//Stage 2 Mapper
 private static class MyMapperStage2 extends Mapper<LongWritable, Text, MyPairOfStrings, FloatWritable> {
   private static final MyPairOfStrings PAIR = new MyPairOfStrings();
   private static final FloatWritable FREQ = new FloatWritable(); // relative frequency

   @Override
   public void map(LongWritable key, Text line, Context context)
       throws IOException, InterruptedException {
     String text = line.toString();
     
     //parsing
     String[] terms = text.split("\\s+"); //parse the intermediate file
     terms[0] = terms[0].substring(1, terms[0].length()-1);
     terms[1] = terms[1].substring(0, terms[1].length()-1);    
     
     PAIR.set(terms[0], terms[1]);
     FREQ.set(Float.parseFloat(terms[2]));
     context.write(PAIR, FREQ);

   }
 }
 
  // Stage 2 Reducer
  private static class MyReducerStage2 extends
  Reducer<MyPairOfStrings, FloatWritable, MyPairOfStrings, FloatWritable> {
  	private final static FloatWritable VAL = new FloatWritable();
  	private static float marginal;

  	@Override
  	public void reduce(MyPairOfStrings key, Iterable<FloatWritable> values, Context context)
  			throws IOException, InterruptedException {

  		float val = values.iterator().next().get();
  		long num = context.getConfiguration().getLong("numRec", 1);

  		if (key.getLeftElement().equals("\1")) {
  			marginal = val / num;
  			VAL.set(marginal);
  		}

  		else {
  			VAL.set((float) Math.log(val/marginal));
  			context.write(key, VAL);
  		}

  	}
  }

  protected static class MyPartitioner extends Partitioner<PairOfStrings, IntWritable> {
  	@Override
  	public int getPartition(PairOfStrings key, IntWritable value, int numReduceTasks) {
  		return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
  	}
  }

  protected static class MyPartitionerStage2 extends Partitioner<MyPairOfStrings, FloatWritable> {
  	@Override
  	public int getPartition(MyPairOfStrings key, FloatWritable value, int numReduceTasks) {
  		return (key.getRightElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
  	}
  }
  
  /**
   * Creates an instance of this tool.
   */
  public PairsPMI() {}

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String NUM_REDUCERS = "numReducers";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
  	// first mapreduce
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output path").create(OUTPUT));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("number of reducers").create(NUM_REDUCERS));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    /*
     * First MapReduce job
     */
    String inputPath = cmdline.getOptionValue(INPUT);
    String outputPath = cmdline.getOptionValue(OUTPUT);
    int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ?
        Integer.parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 1;

    LOG.info("Tool: " + PairsPMI.class.getSimpleName());
    LOG.info("first stage of MapReduce");
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - tmp path: " + outputPath + "/tmp");
    LOG.info(" - number of reducers: " + reduceTasks);

    Job job = Job.getInstance(getConf());
    job.setJobName(PairsPMI.class.getSimpleName());
    job.setJarByClass(PairsPMI.class);
    
 // Delete the tmp directory if it exists already
    Path tmpDir = new Path("tmp_qiwang321");
    FileSystem.get(getConf()).delete(tmpDir, true);

    job.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path("tmp_qiwang321"));

    job.setMapOutputKeyClass(PairOfStrings.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(PairOfStrings.class);
    job.setOutputValueClass(FloatWritable.class);

    job.setMapperClass(MyMapper.class);
    job.setCombinerClass(MyCombiner.class);
    job.setReducerClass(MyReducer.class);
    job.setPartitionerClass(MyPartitioner.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    double time1 = (System.currentTimeMillis() - startTime) / 1000.0;
    System.out.println("Job Finished in " + time1 + " seconds");
    numRecords = job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter",
    		"MAP_INPUT_RECORDS").getValue();
    
    /*
     * Second MapReduce job 
     */
    

    LOG.info("Tool: " + PairsPMI.class.getSimpleName());
    LOG.info("second stage of MapReduce");
    LOG.info(" - tmp path: " + "tmp_qiwang321");
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - number of reducers: " + reduceTasks);

    // set the global variable
    Configuration conf = getConf();
    conf.setLong("numRec", numRecords);
    
    job = Job.getInstance(getConf());
    job.setJobName(PairsPMI.class.getSimpleName());
    job.setJarByClass(PairsPMI.class);

    // Delete the output directory if it exists already
    Path outputDir = new Path(outputPath);
    FileSystem.get(getConf()).delete(outputDir, true);

    job.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job, new Path("tmp_qiwang321/part*"));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setMapOutputKeyClass(MyPairOfStrings.class);
    job.setMapOutputValueClass(FloatWritable.class);
    job.setOutputKeyClass(MyPairOfStrings.class);
    job.setOutputValueClass(FloatWritable.class);

    job.setMapperClass(MyMapperStage2.class);
    //job.setCombinerClass(MyReducerStage2.class);
    job.setReducerClass(MyReducerStage2.class);
    job.setPartitionerClass(MyPartitionerStage2.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    
    startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    double time2 = (System.currentTimeMillis() - startTime) / 1000.0;
    System.out.println("Job Finished in " + time2 + " seconds");
    System.out.println("Total time: " + (time1 + time2) + " seconds");
    numRecords = job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter",
    		"MAP_INPUT_RECORDS").getValue();
    
    // Delete the tmp directory
    FileSystem.get(getConf()).delete(tmpDir, true);
    
    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new PairsPMI(), args);  
  }
}