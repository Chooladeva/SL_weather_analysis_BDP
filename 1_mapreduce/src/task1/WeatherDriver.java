import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WeatherDriver {

    public static void main(String[] args) throws Exception {

        // Validate command-line input parameters (need input + output)
        if (args.length != 2) {
            System.err.println("Usage: WeatherDriver <input_path> <output_path>");
            System.exit(-1);
        }

        // Create Hadoop configuration
        Configuration conf = new Configuration();

        // Create and name the MapReduce job
        Job job = Job.getInstance(conf, "Weather Join & Aggregation");

        // Set the driver class (required for job JAR)
        job.setJarByClass(WeatherDriver.class);

        // Register Mapper and Reducer classes
        job.setMapperClass(WeatherJoinMapper.class);
        job.setReducerClass(WeatherJoinReducer.class);

        // Mapper output types
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Final output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set input and output paths given by user
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Run the job and exit based on success
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
