import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/**
 * This class is used for sorting file data using hadoop.
 * @author priyanka
 *
 */
public class SortMain extends Configured implements Tool{
	
	public static void main(String[] args) throws Exception {
        int r = ToolRunner.run(new Configuration(), new SortMain(), args);
        System.exit(r);
    }

	 @Override
	 public int run(String[] args) throws Exception {
	 		  int r = 0;
		if(args !=null && args.length == 2)
		{			
			Configuration configuration = getConf();
			configuration.set("mapred.textoutputformat.separator", " ");
	        
			FileSystem fileSystem = FileSystem.get(configuration);
			if(fileSystem.exists(new Path(args[1]))){
				fileSystem.delete(new Path(args[1]), true);
			}

			//create job object from the given configuration
			Job hadoopJob = Job.getInstance(configuration);
			hadoopJob.setJobName("Sort hadoop application");
			hadoopJob.setJarByClass(SortMain.class);
			//set the partitioner class 
			hadoopJob.setPartitionerClass(SortPartitions.class);

			//set the Text type for key and value of output
			hadoopJob.setMapOutputKeyClass(Text.class);
			hadoopJob.setMapOutputValueClass(Text.class);

			//set SortMapper class as mapper
			hadoopJob.setMapperClass(SortMapper.class);
			
			//set SortReducer class as Reducer
			hadoopJob.setReducerClass(SortReducer.class);

			// set input path
			FileInputFormat.addInputPath(hadoopJob, new Path(args[0]));
			
			//set output path
			FileOutputFormat.setOutputPath(hadoopJob, new Path(args[1]));


			//wait till job complete
			r = hadoopJob.waitForCompletion(true)?0:1;			
		}
		else{
			System.out.println(" Invalid number of arguments are passed. ");
		}
		return r;
	}
	 
}