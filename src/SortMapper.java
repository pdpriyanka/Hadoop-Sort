import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * This class is used as Mapper for sorting.
 * Each line of input file is split into 1initial 10 byte as a key and remaining 90 bytes as value.
 * @author priyanka
 *
 */
public class SortMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// fetch the first 10 byte as key
		Text keyOutput = new Text(value.toString().substring(0, 10));
		
		// remaining 90 bytes as value
		Text valueOutput = new Text(value.toString().substring(keyOutput.getLength()));
		context.write(keyOutput, valueOutput);
	}
}
