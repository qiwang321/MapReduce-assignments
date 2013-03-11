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
import java.text.DecimalFormat;
import java.text.NumberFormat;
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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;
import edu.umd.cloud9.mapreduce.lib.input.NonSplitableSequenceFileInputFormat;
//import edu.umd.cloud9.util.map.HMapIF;
import edu.umd.cloud9.util.map.HMapIV;
//import edu.umd.cloud9.util.map.MapIF;
import edu.umd.cloud9.util.map.MapIV;
//import edu.umd.cloud9.util.map.MapIV.Entry;

/**
 * <p>
 * Main driver program for running the basic (non-Schimmy) implementation of
 * PageRank.
 * </p>
 *
 * <p>
 * The starting and ending iterations will correspond to paths
 * <code>/base/path/iterXXXX</code> and <code>/base/path/iterYYYY</code>. As a
 * example, if you specify 0 and 10 as the starting and ending iterations, the
 * driver program will start with the graph structure stored at
 * <code>/base/path/iter0000</code>; final results will be stored at
 * <code>/base/path/iter0010</code>.
 * </p>
 *
 * @see RunPageRankSchimmy
 * @author Jimmy Lin
 * @author Michael Schatz
 */
public class RunPersonalizedPageRankBasic extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(RunPersonalizedPageRankBasic.class);
  
  // Mapper with in-mapper combiner optimization.
  private static class MapWithInMapperCombiningClass extends
      Mapper<IntWritable, MyPageRankNode, IntWritable, MyPageRankNode> {
    // For buffering PageRank mass contributes keyed by destination node.
    private static HMapIV<float[]> map = new HMapIV<float[]>();

    // For passing along node structure.
    private static final MyPageRankNode intermediateStructure = new MyPageRankNode();
    
    // source nodes
    private static int[] sources;
    private static int ns; // number of sources
    private static float[] totalMass;
    
    @Override 
    public void setup(Context context) {  	
    	Configuration conf = context.getConfiguration();
      ns = conf.getInt("sourceNum", 0);
      sources = new int[ns];
      totalMass = new float[ns];
      for (int k =0; k < ns; k++) {
      	sources[k] = conf.getInt(Integer.toString(k), 0);
      	totalMass[k] = Float.NEGATIVE_INFINITY;
      }
    }

    @Override
    public void map(IntWritable nid, MyPageRankNode node, Context context)
        throws IOException, InterruptedException {
      // Pass along node structure.
      intermediateStructure.setNodeId(node.getNodeId());
      intermediateStructure.setType(MyPageRankNode.Type.Structure);
      intermediateStructure.setAdjacencyList(node.getAdjacenyList());

      context.write(nid, intermediateStructure);

      // Distribute PageRank mass to neighbors (along outgoing edges).
      if (node.getAdjacenyList().size() > 0) {
        // Each neighbor gets an equal share of PageRank mass.
        ArrayListOfIntsWritable list = node.getAdjacenyList();
        float[] mass = new float[ns];
        float[] sum = new float[ns];
        for (int k = 0; k < ns; k++) {
        	mass[k] = node.getPageRank()[k] - (float) StrictMath.log(list.size());
        	totalMass[k] = sumLogProbs(totalMass[k], node.getPageRank()[k]);
        }


        // Iterate over neighbors.
        for (int i = 0; i < list.size(); i++) {
          int neighbor = list.get(i);

          if (map.containsKey(neighbor)) {
            // Already message destined for that node; add PageRank mass contribution.
            for (int k = 0; k < ns; k++)
            	sum[k] = sumLogProbs(map.get(neighbor)[k], mass[k]);
            map.put(neighbor, sum);
          } 
          
          else {
            // New destination node; add new entry in map.
            map.put(neighbor, mass);
          }
        }
      }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      // Now emit the messages all at once.
      IntWritable key = new IntWritable();
      MyPageRankNode value = new MyPageRankNode();

      for (MapIV.Entry<float[]> e : map.entrySet()) {
        key.set(e.getKey());

        value.setNodeId(e.getKey());
        value.setType(MyPageRankNode.Type.Mass);
        value.setPageRank(e.getValue());

        context.write(key, value);
      }
      
      // write out the totalMass side data
      Configuration conf = context.getConfiguration();
      String taskId = conf.get("mapred.task.id");
      String path = conf.get("PageRankMassPath");

      Preconditions.checkNotNull(taskId);
      Preconditions.checkNotNull(path);

      // Write to a file the amount of PageRank mass we've seen in this mapper.
      FileSystem fs = FileSystem.get(context.getConfiguration());
      FSDataOutputStream out = fs.create(new Path(path + "/" + taskId), false);
      for (int k = 0; k < ns; k++)
      	out.writeFloat(totalMass[k]);
      out.close();
      
    }
  }

  // Combiner: sums partial PageRank contributions and passes node structure along.
  private static class CombineClass extends
      Reducer<IntWritable, MyPageRankNode, IntWritable, MyPageRankNode> {
    private static final MyPageRankNode intermediateMass = new MyPageRankNode();
    
    // source nodes
    private static int ns; // number of sources
    private static int[] sources;
    
    @Override 
    public void setup(Context context) {  	
    	Configuration conf = context.getConfiguration();
      ns = conf.getInt("sourceNum", 0);
      sources = new int[ns];
      for (int k =0; k < ns; k++)
      	sources[k] = conf.getInt(Integer.toString(k), 0);
    }

    @Override
    public void reduce(IntWritable nid, Iterable<MyPageRankNode> values, Context context)
        throws IOException, InterruptedException {
      int massMessages = 0;
      // Remember, PageRank mass is stored as a log prob.
      float[] mass = new float[ns];
      for (int k = 0; k < ns; k++)
      	mass[k] = Float.NEGATIVE_INFINITY;
      for (MyPageRankNode n : values) {
        if (n.getType() == MyPageRankNode.Type.Structure) {
          // Simply pass along node structure.
          context.write(nid, n);
        } 
        else {
          // Accumulate PageRank mass contributions.
        	for (int k = 0; k < ns; k++)
        		mass[k] = sumLogProbs(mass[k], n.getPageRank()[k]);
        	massMessages++;
        }
      }

      // Emit aggregated results.
      if (massMessages > 0) {
        intermediateMass.setNodeId(nid.get());
        intermediateMass.setType(MyPageRankNode.Type.Mass);
        intermediateMass.setPageRank(mass);

        context.write(nid, intermediateMass);
      }
    }
  }

  // Reduce: sums incoming PageRank contributions, rewrite graph structure.
  private static class ReduceClass extends
      Reducer<IntWritable, MyPageRankNode, IntWritable, MyPageRankNode> {
    // For keeping track of PageRank mass encountered, so we can compute missing PageRank mass lost
    // through dangling nodes.
    private static float[] totalMass;
    		

    // source nodes
    private static int[] sources;
    private static int ns; // number of sources
    
    
    @Override 
    public void setup(Context context) {  	
    	Configuration conf = context.getConfiguration();
      ns = conf.getInt("sourceNum", 0);
      sources = new int[ns];
      totalMass = new float[ns];
      for (int k =0; k < ns; k++) {
      	sources[k] = conf.getInt(Integer.toString(k), 0);
      	totalMass[k] = Float.NEGATIVE_INFINITY;
      }
      
      // read in the totalMass information
      String outm = conf.get("PageRankMassPath");      
      try {
      	FileSystem fs = FileSystem.get(conf);
      	for (FileStatus f : fs.listStatus(new Path(outm))) {
      		FSDataInputStream fin = fs.open(f.getPath());
      		for (int k = 0; k < ns; k++)
      			totalMass[k] = sumLogProbs(totalMass[k], fin.readFloat());
      		fin.close();
      	}
      } catch (IOException e) {
      	// TODO Auto-generated catch block
      	e.printStackTrace();
      }
    }

    @Override
    public void reduce(IntWritable nid, Iterable<MyPageRankNode> iterable, Context context)
        throws IOException, InterruptedException {
      Iterator<MyPageRankNode> values = iterable.iterator();

      // Create the node structure that we're going to assemble back together from shuffled pieces.
      MyPageRankNode node = new MyPageRankNode();

      node.setType(MyPageRankNode.Type.Complete);
      node.setNodeId(nid.get()); 

      float[] mass = new float[ns];
      for (int k = 0; k < ns; k++) {
      	mass[k] = Float.NEGATIVE_INFINITY;
      	if (nid.get() == sources[k])
      		mass[k] = (float) StrictMath.log(ALPHA * StrictMath.exp(totalMass[k])
      				+ (1.0f-(float) StrictMath.exp(totalMass[k]))); // missing mass
      }
      
      int structureReceived = 0;
      int massMessagesReceived = 0;
      
      while (values.hasNext()) {
        MyPageRankNode n = values.next();

        if (n.getType().equals(MyPageRankNode.Type.Structure)) {
          // This is the structure; update accordingly.
          ArrayListOfIntsWritable list = n.getAdjacenyList();
          structureReceived++;

          node.setAdjacencyList(list);
        } else {
          // This is a message that contains PageRank mass; accumulate.
        	for (int k = 0; k < ns; k++)
        		mass[k] = sumLogProbs(mass[k], n.getPageRank()[k]);
        }
      }

      // Update the final accumulated PageRank mass.
      for (int k = 0; k < ns; k++)
    		mass[k] = mass[k] + (float) StrictMath.log(1.0f - ALPHA);
      node.setPageRank(mass);
 
      // Error checking.
      if (structureReceived == 1) {
        // Everything checks out, emit final node structure with updated PageRank value.
        context.write(nid, node);

      } else if (structureReceived == 0) { // migrated to the mapper
        // We get into this situation if there exists an edge pointing to a node which has no
        // corresponding node structure (i.e., PageRank mass was passed to a non-existent node)...
        // log and count but move on.
        //context.getCounter(MyPageRank.missingStructure).increment(1);
        LOG.warn("No structure received for nodeid: " + nid.get() + " mass: "
            + massMessagesReceived);
        // It's important to note that we don't add the PageRank mass to total... if PageRank mass
        // was sent to a non-existent node, it should simply be redirected to the source node.
        
      } else {
        // This shouldn't happen!
        throw new RuntimeException("Multiple structure received for nodeid: " + nid.get()
            + " mass: " + massMessagesReceived + " struct: " + structureReceived);
      }
    }
  }

  // Random jump factor.
  private static float ALPHA = 0.15f;
  private static NumberFormat formatter = new DecimalFormat("0000");


	/**
	 * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new RunPersonalizedPageRankBasic(), args);
	}

	public RunPersonalizedPageRankBasic() {}

  private static final String BASE = "base";
  private static final String NUM_NODES = "numNodes";
  private static final String START = "start";
  private static final String END = "end";
  private static final String SOURCES = "sources";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("base path").create(BASE));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("start iteration").create(START));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("end iteration").create(END));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("number of nodes").create(NUM_NODES));
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

    if (!cmdline.hasOption(BASE) || !cmdline.hasOption(START) ||
        !cmdline.hasOption(END) || !cmdline.hasOption(NUM_NODES) ||
        !cmdline.hasOption(SOURCES)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

		String basePath = cmdline.getOptionValue(BASE);
		String sourceString = cmdline.getOptionValue(SOURCES);
		int n = Integer.parseInt(cmdline.getOptionValue(NUM_NODES));
		int s = Integer.parseInt(cmdline.getOptionValue(START));
		int e = Integer.parseInt(cmdline.getOptionValue(END));

    LOG.info("Tool name: RunPageRank");
    LOG.info(" - base path: " + basePath);
    LOG.info(" - num nodes: " + n);
    LOG.info(" - start iteration: " + s);
    LOG.info(" - end iteration: " + e);
    
    //parse the sources string
    Configuration conf = getConf();
    String[] str = sourceString.split(",");
    int ns = str.length;
    conf.setInt("sourceNum", ns);   
    int[] sources = new int[ns];
    for (int k = 0; k < ns; k++) {
    	sources[k] = Integer.parseInt(str[k]);
    	conf.setInt(Integer.toString(k), sources[k]);
    }

    // Iterate PageRank.
    for (int i = s; i < e; i++) {
      iteratePageRank(i, i + 1, basePath, n, ns, sources, conf, i, e-1);
    }

    return 0;
  }

  // Run each iteration.
  private void iteratePageRank(int i, int j, String basePath, int numNodes, int numSources,
      int[] sources, Configuration conf, int iter, int end) throws Exception {
    // Each iteration consists of two phases (two MapReduce jobs).

    // Job 1: distribute PageRank mass along outgoing edges.
    //float[] mass = phase1(i, j, basePath, numNodes, numSources,conf);
  	phase1(i, j, basePath, numNodes, numSources,conf);
  	
    /*// Find out how much PageRank mass got lost at the dangling nodes.
    float[] missing = new float[numSources];
    for (int k = 0; k < numSources; k++) {
    	missing[k] = 1.0f - (float) StrictMath.exp(mass[k]);
    	conf.setFloat("m" + Integer.toString(k), missing[k]);
    }*/

    // Job 2: distribute missing mass, take care of random jump factor.
    //phase2(i, j, basePath, numNodes, numSources, conf, iter, end);
  }

  private void phase1(int i, int j, String basePath, int numNodes, int numSources,
      Configuration conf) throws Exception {
    Job job = Job.getInstance(conf);
    job.setJobName("PageRank:Basic:iteration" + j);
    job.setJarByClass(RunPersonalizedPageRankBasic.class);

    String in = basePath + "/iter" + formatter.format(i);
    //String out = basePath + "/iter" + formatter.format(j) + "t";
    String out = basePath + "/iter" + formatter.format(j);
    String outm = out + "-mass";

    // We need to actually count the number of part files to get the number of partitions (because
    // the directory might contain _log).
    int numPartitions = 0;
    for (FileStatus s : FileSystem.get(getConf()).listStatus(new Path(in))) {
      if (s.getPath().getName().contains("part-"))
        numPartitions++;
    }

    LOG.info("PageRank: iteration " + j + ": Phase1");
    LOG.info(" - input: " + in);
    LOG.info(" - output: " + out);
    LOG.info(" - nodeCnt: " + numNodes);

    int numReduceTasks = numPartitions;

    //job.getConfiguration().setInt("NodeCount", numNodes);
    job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);
    job.getConfiguration().setBoolean("mapred.reduce.tasks.speculative.execution", false);
    //job.getConfiguration().set("mapred.child.java.opts", "-Xmx2048m");
    job.getConfiguration().set("PageRankMassPath", outm);
    //job.getConfiguration().set("Sources", sources); // set global parameter specifying sources
    //String[] s = sources.split(","); // split the source indices
    
    //job.getConfiguration().setStrings("sourceNodes", s); // side data for source indices
    //job.getConfiguration().setInt("sourceNum", s.length); // number of source nodes

    job.setNumReduceTasks(numReduceTasks);

    FileInputFormat.setInputPaths(job, new Path(in));
    FileOutputFormat.setOutputPath(job, new Path(out));

    job.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(MyPageRankNode.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(MyPageRankNode.class);

    job.setMapperClass(MapWithInMapperCombiningClass.class);
    job.setCombinerClass(CombineClass.class);
    job.setReducerClass(ReduceClass.class);
    job.setPartitionerClass(RangePartitioner.class);

    FileSystem.get(getConf()).delete(new Path(out), true);
    FileSystem.get(getConf()).delete(new Path(outm), true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    /*float mass[] = new float[numSources];
    for (int k = 0; k < numSources; k++)
    	mass[k] = Float.NEGATIVE_INFINITY;
    FileSystem fs = FileSystem.get(getConf());
    for (FileStatus f : fs.listStatus(new Path(outm))) {
      FSDataInputStream fin = fs.open(f.getPath());
      for (int k = 0; k < numSources; k++)
      	mass[k] = sumLogProbs(mass[k], fin.readFloat());
      fin.close();
    }*/

    //return mass;
  }

  private void phase2(int i, int j, String basePath, int numNodes,
  		int numSources, Configuration conf, int iter, int end) throws Exception {
    Job job = Job.getInstance(conf);
    job.setJobName("PageRank:Basic:iteration" + j + ":Phase2");
    job.setJarByClass(RunPersonalizedPageRankBasic.class);

    //LOG.info("missing PageRank mass: " + missing);
    LOG.info("number of nodes: " + numNodes);

    String in = basePath + "/iter" + formatter.format(j) + "t";
    String out = basePath + "/iter" + formatter.format(j);

    LOG.info("PageRank: iteration " + j + ": Phase2");
    LOG.info(" - input: " + in);
    LOG.info(" - output: " + out);

    job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);
    job.getConfiguration().setBoolean("mapred.reduce.tasks.speculative.execution", false);

    job.setNumReduceTasks(0);

    FileInputFormat.setInputPaths(job, new Path(in));
    FileOutputFormat.setOutputPath(job, new Path(out));
    
    job.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(MyPageRankNode.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(MyPageRankNode.class);

    FileSystem.get(getConf()).delete(new Path(out), true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
  }

  
  // Adds two log probs.
  private static float sumLogProbs(float a, float b) {
    if (a == Float.NEGATIVE_INFINITY)
      return b;

    if (b == Float.NEGATIVE_INFINITY)
      return a;

    if (a < b) {
      return (float) (b + StrictMath.log1p(StrictMath.exp(a - b)));
    }

    return (float) (a + StrictMath.log1p(StrictMath.exp(b - a)));
  }
  
}
