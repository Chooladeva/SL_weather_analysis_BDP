import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WeatherJoinReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        // Will store the city name (only 1 expected per location_id)
        String cityName = null;

        // Store weather rows temporarily until we know the city
        ArrayList<String> weatherRows = new ArrayList<>();

        // --------------------------------------------------------
        // Step 1: Separate location (L|...) and weather (W|...) data
        // --------------------------------------------------------
        for (Text v : values) {
            String line = v.toString();

            if (line.startsWith("L|")) {
                // Extract city name from "L|cityName"
                cityName = line.split("\\|")[1];
            } 
            else if (line.startsWith("W|")) {
                // Add weather record for future processing
                weatherRows.add(line);
            }
        }

        // If city is missing, no join possible → skip
        if (cityName == null) return;

        // --------------------------------------------------------
        // Step 2: Aggregate weather by (city, year, month)
        // --------------------------------------------------------

        // Helper class to store intermediate sums
        class Stats {
            double precipSum = 0;
            double tempSum = 0;
            int count = 0;
        }

        // HashMap: "city,year,month" → Stats object
        java.util.HashMap<String, Stats> aggregation = new java.util.HashMap<>();

        // Process each weather record
        for (String w : weatherRows) {
            String[] parts = w.split("\\|");
            if (parts.length < 4) continue;

            // W|date|tempMean|precipHours
            String date = parts[1];
            String tempMean = parts[2];
            String precipHours = parts[3];

            // Skip invalid numbers
            if (tempMean.isEmpty() || precipHours.isEmpty()) continue;

            // Extract month, year from dd/mm/yyyy
            String[] d = date.split("/");
            if (d.length < 3) continue;

            String month = String.format("%02d", Integer.parseInt(d[1]));
            String year = d[2];

            // Build grouping key → city,year,month
            String mapKey = cityName + "," + year + "," + month;

            double temp = 0.0;
            double precip = 0.0;

            try {
                temp = Double.parseDouble(tempMean);
                precip = Double.parseDouble(precipHours);
            } catch (Exception ignored) {
                // Skip bad numeric rows silently
            }

            // Get existing stats or create new
            Stats s = aggregation.getOrDefault(mapKey, new Stats());

            // Accumulate values
            s.tempSum += temp;
            s.precipSum += precip;
            s.count += 1;

            aggregation.put(mapKey, s);
        }

        // --------------------------------------------------------
        // Step 3: Emit final results per (city,year,month)
        // --------------------------------------------------------
        for (String k : aggregation.keySet()) {
            Stats s = aggregation.get(k);

            // Calculate monthly average temperature
            double avgTemp = s.tempSum / s.count;

            String output =
                    "Total Precipitation=" + s.precipSum +
                    ", Mean Temperature=" + avgTemp;

            // Output format:
            // key:   city,year,month
            // value: Total Precipitation=..., Mean Temperature=...
            context.write(new Text(k), new Text(output));
        }
    }
}
