import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WeatherJoinMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // Read one line from the CSV
        String line = value.toString().trim();
        if (line.length() == 0) return;

        // Ignore header lines from both CSVs
        if (line.toLowerCase().contains("location_id")) return;

        // Detect which file the line came from
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

        // Split by comma
        String[] fields = line.split(",", -1);

        // ------------------------------
        // Handle locationData.csv
        // ------------------------------
        if (fileName.toLowerCase().contains("location")) {

            // Expect at least 8 columns (city name is column 7)
            if (fields.length < 8) return;

            String locationId = fields[0].trim();
            String cityName = fields[7].trim();

            // Emit: key = location_id, value = "L|cityName"
            if (!locationId.isEmpty() && !cityName.isEmpty()) {
                context.write(new Text(locationId), new Text("L|" + cityName));
            }

        } else {

            // ------------------------------
            // Handle weatherData.csv
            // ------------------------------
            if (fields.length < 14) return;

            String locationId = fields[0].trim();
            String date = fields[1].trim();
            String tempMean = fields[5].trim();      // temperature_2m_mean
            String precipHours = fields[13].trim();  // precipitation_hours

            if (locationId.isEmpty() || date.isEmpty()) return;

            // --- Extract year from date ---
            String[] dateParts = date.split("/");
            if (dateParts.length != 3) return;  // skip malformed dates

            int year;
            try {
                year = Integer.parseInt(dateParts[2]);
            } catch (NumberFormatException e) {
                return;
            }

            // --- Filter for past decade 2010-2020 ---
            if (year < 2010 || year > 2019) return;

            // Emit: key = location_id, value = "W|date|tempMean|precipHours"
            context.write(
                new Text(locationId),
                new Text("W|" + date + "|" + tempMean + "|" + precipHours)
            );
        }
    }
}
