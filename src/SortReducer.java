import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * This class is used as reducer for sorting.
 * It takes the value for specific key and write key value into context.
 * @author priyanka
 *
 */
public class SortReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		//iterate over values of specific key and write key value into context
		// \r is appended as valsort program need \r\n as new line
		for (Text text : values) {
			context.write(key, new Text(text.toString().substring(1)+"\r"));
		}
	}

}
