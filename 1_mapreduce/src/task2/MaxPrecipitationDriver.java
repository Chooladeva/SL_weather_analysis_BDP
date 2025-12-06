import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxPrecipitationDriver {

    public static void main(String[] args) throws Exception {

        // Ensure correct number of arguments (input and output paths)
        if (args.length != 2) {
            System.err.println("Usage: MaxPrecipitationDriver <input path> <output path>");
            System.exit(-1);
        }

        // Create Hadoop configuration
        Configuration conf = new Configuration();

        // Create and name the MapReduce job
        Job job = Job.getInstance(conf, "Max Precipitation per Month");

        // Set the driver class (required for packaging JAR)
        job.setJarByClass(MaxPrecipitationDriver.class);

        // Set Mapper and Reducer classes
        job.setMapperClass(MaxPrecipitationMapper.class);
        job.setReducerClass(MaxPrecipitationReducer.class);

        // Set output key/value types for the job
        job.setOutputKeyClass(Text.class);          // month-year as key
        job.setOutputValueClass(DoubleWritable.class); // total precipitation as value

        // Set input and output paths from command-line arguments
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Submit job and exit with appropriate status
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
