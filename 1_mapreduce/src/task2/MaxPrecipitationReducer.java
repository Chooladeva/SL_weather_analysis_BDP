import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxPrecipitationReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    private String maxMonthYear = null;
    private double maxPrecip = 0.0;

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {

        double totalPrecip = 0.0;

        // Sum all precipitation values for this month-year
        for (DoubleWritable val : values) {
            totalPrecip += val.get();
        }

        // Update max if current total exceeds previous max
        if (totalPrecip > maxPrecip) {
            maxPrecip = totalPrecip;
            maxMonthYear = key.toString();
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Emit only the month-year with the highest total precipitation
        if (maxMonthYear != null) {
            context.write(new Text(maxMonthYear), new DoubleWritable(maxPrecip));
        }
    }
}
