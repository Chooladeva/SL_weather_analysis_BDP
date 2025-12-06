import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxPrecipitationMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {

        // Read a line from the CSV
        String line = value.toString().trim();
        if (line.isEmpty()) return; // skip empty lines

        // Skip header line if it contains 'location_id' or 'date'
        if (line.toLowerCase().contains("location_id") || line.toLowerCase().contains("date")) return;

        // Split CSV line
        String[] fields = line.split(",", -1); // -1 to keep empty fields

        try {
            // Extract date and precipitation_hours
            String date = fields[1].trim();           // format in dataset: m/d/yyyy
            double precipitation = Double.parseDouble(fields[13].trim()); // precipitation_hours

            // Extract month and year from date
            String[] dateParts = date.split("/");  
            if (dateParts.length != 3) return;       // skip malformed dates

            String day = dateParts[0];               // d
            String month = String.format("%02d", Integer.parseInt(dateParts[1])); // m
            String year = dateParts[2];              // yyyy

            // Emit key = "year,month", value = precipitation
            context.write(new Text(year + "," + month), new DoubleWritable(precipitation));

        } catch (Exception e) {
            // Skip lines with parsing errors
        }
    }
}
