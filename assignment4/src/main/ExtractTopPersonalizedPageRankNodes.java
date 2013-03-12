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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.pair.PairOfInts;
import edu.umd.cloud9.util.TopNScoredObjects;
import edu.umd.cloud9.util.pair.PairOfObjectFloat;

public class ExtractTopPersonalizedPageRankNodes extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(ExtractTopPersonalizedPageRankNodes.class);

  private static class MyMapper extends
      Mapper<IntWritable, MyPageRankNode, PairOfInts, FloatWritable> {
    private ArrayList<TopNScoredObjects<Integer>> queues;
    private static int ns;
    private static int[] sources;
    private static int nt; // number of tops shown
    private static final PairOfInts key = new PairOfInts();
    private static final FloatWritable value = new FloatWritable();

    @Override
    public void setup(Context context) throws IOException {
    	Configuration conf = context.getConfiguration();
      nt = conf.getInt("n", 100);
      ns = conf.getInt("sourceNum", 0);
      sources = new int[ns];
      queues = new ArrayList<TopNScoredObjects<Integer>>();
      for (int k = 0; k < ns; k++) {
      	sources[k] = conf.getInt(Integer.toString(k), 0);
      	queues.add(new TopNScoredObjects<Integer>(nt));
      }
      
    }

    @Override
    public void map(IntWritable nid, MyPageRankNode node, Context context) throws IOException,
        InterruptedException {
    	for (int k = 0; k < ns; k++)
    		queues.get(k).add(node.getNodeId(), node.getPageRank()[k]);
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {

    	for (int k = 0; k < ns; k++) {
    		for (PairOfObjectFloat<Integer> pair : queues.get(k).extractAll()) {
    			key.set(k, pair.getLeftElement());
    			value.set(pair.getRightElement());
    			context.write(key, value);
    		}
    	}
    }
  }

  private static class MyReducer extends
      Reducer<PairOfInts, FloatWritable, IntWritable, FloatWritable> {
    private static ArrayList<TopNScoredObjects<Integer>> queues;
    private static int ns;
    private static int[] sources;
    private static int nt; // number of tops shown
    private static final IntWritable key = new IntWritable();
    private static final FloatWritable value = new FloatWritable();
    

    @Override
    public void setup(Context context) throws IOException {
    	Configuration conf = context.getConfiguration();
      nt = conf.getInt("n", 100);
      ns = conf.getInt("sourceNum", 0);
      sources = new int[ns];
      queues = new ArrayList<TopNScoredObjects<Integer>>(ns);
      for (int k = 0; k < ns; k++) {
      	sources[k] = conf.getInt(Integer.toString(k), 0);
      	queues.add(new TopNScoredObjects<Integer>(nt));
      }
    }

    @Override
    public void reduce(PairOfInts nid, Iterable<FloatWritable> iterable, Context context)
        throws IOException {
      Iterator<FloatWritable> iter = iterable.iterator();
      queues.get(nid.getLeftElement()).add(nid.getRightElement(), iter.next().get());


      // Shouldn't happen. Throw an exception.
      if (iter.hasNext()) {
        throw new RuntimeException();
      }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {

    	for (int k = 0; k < ns; k++) {
    		for (PairOfObjectFloat<Integer> pair : queues.get(k).extractAll()) {
    			key.set(pair.getLeftElement());
    			value.set((float) Math.exp(pair.getRightElement()));
    			context.write(key, value);
    		}   		
    	}
    }
  }

  public ExtractTopPersonalizedPageRankNodes() {
  }

  private static final String INPUT = "input";
  private static final String TOP = "top";
  private static final String SOURCES = "sources";
  
  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("top n").create(TOP));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("source nodes").create(SOURCES));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(TOP) 
    		|| !cmdline.hasOption(SOURCES)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String inputPath = cmdline.getOptionValue(INPUT);
    Path input = new Path(inputPath);
    Path tmp = new Path("qiwang321-tmp");
    // Delete the tmp directory if it exists already
    FileSystem.get(getConf()).delete(tmp, true);

    int n = Integer.parseInt(cmdline.getOptionValue(TOP));
    String sourceString = cmdline.getOptionValue(SOURCES);

    LOG.info("Tool name: " + ExtractTopPersonalizedPageRankNodes.class.getSimpleName());
    LOG.info(" - input: " + inputPath);
    LOG.info(" - tmp: qiwang321-tmp");
    LOG.info(" - top: " + n);

    Configuration conf = getConf();
    conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024);
    conf.setInt("n", n);
    String[] str = sourceString.split(",");
    int ns = str.length;
    int[] sources = new int[ns];
    conf.setInt("sourceNum", ns);
    for (int k = 0; k < ns; k++) {
    	sources[k] = Integer.parseInt(str[k]);
    	conf.setInt(Integer.toString(k), sources[k]);
    }
    

    Job job = Job.getInstance(conf);
    job.setJobName(ExtractTopPersonalizedPageRankNodes.class.getName() + ":" + inputPath);
    job.setJarByClass(ExtractTopPersonalizedPageRankNodes.class);

    job.setNumReduceTasks(1);

    FileInputFormat.addInputPath(job, input);
    FileOutputFormat.setOutputPath(job, tmp);

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapOutputKeyClass(PairOfInts.class);
    job.setMapOutputValueClass(FloatWritable.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(FloatWritable.class);

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    FileSystem.get(conf).delete(tmp, true);

    job.waitForCompletion(true);
    
    FileSystem fs = FileSystem.get(conf); 	
  	FSDataInputStream fin = fs.open(new Path("qiwang321-tmp/part-r-00000"));
    BufferedReader in = new BufferedReader(new InputStreamReader(fin)); 
    String[] s;
    for (int k = 0; k < ns; k++) {
    	System.out.println("Source: " + sources[k]);
    	for (int l = 0; l < n; l++) {
    		s = in.readLine().split("\\s+");
    		System.out.println(String.format("%.5f %d", Float.parseFloat(s[1]), Integer.parseInt(s[0])));
    	}
    	System.out.println("");
    }
    
    in.close();
    
    //clean up.
    FileSystem.get(conf).delete(tmp, true);

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new ExtractTopPersonalizedPageRankNodes(), args);
    System.exit(res);
  }
}
