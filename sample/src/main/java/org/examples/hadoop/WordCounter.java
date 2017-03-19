package org.examples.hadoop;

import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.io.MongoUpdateWritable;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.mongodb.hadoop.util.MongoTool;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.bson.BasicBSONObject;

/**
 *
 * Simple Hadoop example. Counts words from any text file
 * and saves to MongoDB.
 * 
 * @author fabianenardon
 */
public class WordCounter extends MongoTool {
    
    public static final String DATABASE = "mongo_hadoop";
    public static final String COLLECTION = "word_count";
    
    public WordCounter() throws UnknownHostException {
    }

    public static class Mapp extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text key = new Text();
        private IntWritable value = new IntWritable();
        
        public Mapp() {
            key.set("total");
        }

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            StringTokenizer st = new StringTokenizer(value.toString(), " ");
            int count = 0;
            while (st.hasMoreTokens()) {
                st.nextToken();
                count++;
            }
            this.value.set(count);
            context.write(this.key, this.value);
        }

    }

    public static class Reduce extends Reducer<Text, IntWritable, NullWritable, MongoUpdateWritable> {
        
        private MongoUpdateWritable reduceResult;
        private String processName = null;
        
        public Reduce() {
            super();
            reduceResult = new MongoUpdateWritable();
        }
        
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            
            // key is the file name
            BasicBSONObject query = new BasicBSONObject("_id", getProcessName(context));
            
            int count = 0;
            for (IntWritable value : values) {
                count = count + value.get();
            }
            
            
            BasicBSONObject update = new BasicBSONObject("$inc", new BasicBSONObject("words", count));
            reduceResult.setQuery(query);
            reduceResult.setModifiers(update);
            
            context.write(null, reduceResult);
            
        }
        
        private String getProcessName(Context context) {
            if (processName != null) {
                return processName;
            }
            processName = context.getJobName();
            return processName;
        }
    }

    /**
     *
     * @param args
     *
     */
    public static void main(String[] args) throws Exception {
        
        String namenode = "hdfs://localhost:9000";
        String folder = "/files";
        String mongoHost = "localhost:27017";
        String jobTracker = "localhost";
        
        if (args.length > 3) {
            jobTracker = args[3];
        } 
        if (args.length > 2) {
            mongoHost = args[2];
        }
        if (args.length > 1) {
            folder = args[1];
        }
        if (args.length > 0) {
            namenode = args[0];
        }
        
        if (args.length < 4) {
            System.out.println("USAGE: hadoop -jar docker-hadoop-example-1.0-SNAPSHOT-precombined-mr.jar <HDFS_ADDRESS> <PATH_TO_HDFS_WHERE_TO_FIND_FILES_TO_PROCESS> <MONGO_SERVER> <YARN_ADDRESS_AND_PORT>");
            System.out.println("       If you are running inside docker, run with hadoop -jar docker-hadoop-example-1.0-SNAPSHOT-mr.jar hdfs://namenode:9000 /files mongo yarn:8050");
            System.out.println("RUNNING WITH "+namenode+" "+folder+" "+mongoHost+" "+jobTracker);
        }

        Configuration config = new Configuration();
        config.set("fs.defaultFS", namenode);
        config.set("mapred.map.tasks.speculative.execution", "false");
        config.set("mapred.reduce.tasks.speculative.execution", "false");
        // It can be local or yarn. Use local for local test
    //    config.set("mapreduce.framework.name", "yarn");
        
        // Important: set this to be able to run on hadoop on docker installed in the host
        config.set("dfs.client.use.datanode.hostname", "true");
        // yarn address (we are mapping to the yarn docker)
        config.set("yarn.resourcemanager.address", jobTracker);
        config.set("mapreduce.jobtracker.address", jobTracker);
        
        System.exit(ToolRunner.run(config, new WordCounter(), new String[] {namenode, folder, mongoHost, jobTracker}));

    }

    @Override
    public int run(String[] args) throws Exception {

        // Finds files that need processing
        FileSystem fs = FileSystem.get(getConf());
        Path path = new Path(args[1]);
        FileStatus[] files = fs.listStatus(path);
        int rc = 0;
        if (files != null && files.length > 0) {

            Job job = Job.getInstance(getConf());
            job.setJarByClass(WordCounter.class);

            job.setJobName("Counts on "+new Date());

            for (FileStatus file : files) {
                FileInputFormat.addInputPath(job, file.getPath());
            }
            
            FileOutputFormat.setOutputPath(job, new Path("/tmp/demos/" + System.currentTimeMillis()));
            
            MongoConfigUtil.setOutputURI(job.getConfiguration(), "mongodb://"+args[2]+"/"+DATABASE+"."+COLLECTION);

            
            job.setMapperClass(WordCounter.Mapp.class);
            job.setReducerClass(WordCounter.Reduce.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(BSONWritable.class);
            job.setOutputFormatClass(MongoOutputFormat.class);
            job.submit();

            rc = (job.waitForCompletion(true)) ? 0 : 1;


        }
        return rc;

    }
}
