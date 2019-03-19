import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class WordCountV2 extends Configured implements Tool{
    public static final Log log = LogFactory.getLog(JSONRecordReader.class);
    /**
     * Main function which calls the run method and passes the args using ToolRunner
     * @param args Two arguments input and output file paths
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new WordCountV2(), args);
        System.exit(exitCode);
    }

    /**
     * Run method which schedules the Hadoop Job
     * @param args Arguments passed in main function
     */
    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.printf("Usage: %s needs two arguments <input> <output> files\n",
                    getClass().getSimpleName());
            System.err.println(String.format("%s %s",args[0].toString(),args[1].toString()));
            return -1;
        }
        //Initialize the Hadoop job and set the jar as well as the name of the Job
        Job job = new Job();
        job.setJarByClass(WordCountV2.class);
        job.setJobName("BiGram WordCount");

        job.setInputFormatClass(JSONInputFormat.class);
        JSONInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //Set the MapClass and ReduceClass in the job
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        //Wait for the job to complete and print if the job was successful or not
        int returnValue = job.waitForCompletion(true) ? 0:1;

        if(job.isSuccessful()) {
            System.out.println("Job was successful");
        } else if(!job.isSuccessful()) {
            System.out.println("Job was not successful");
        }

        return returnValue;
    }

    public static class MyMapper extends Mapper<Text, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Text key, Text value, Context context
        ) throws IOException, InterruptedException {
            log.info("In Mapper ==================================================================================");
            log.info("value=============="+value);
            context.write(value,one);
        }
    }

    public static class MyReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            log.info("In Reducer=====================================================================================");
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            log.info("key============="+key+" result====================="+result);
            context.write(key, result);
        }
    }

}