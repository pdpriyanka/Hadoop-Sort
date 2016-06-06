import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
/**
 * This class is used to partition the mapped data as per key.
 * @author priyanka
 *
 */
public class SortPartitions extends Partitioner<Text, Text>{

	@Override
	public int getPartition(Text key, Text value, int noOfPartitions) {
		/**
		 * keys between ASCII 32 to 126 are split into parts.
		 * 
		 * 1. ASCII value of first byte of key < 32 then assign that key to partition 0
		 * 2. ASCII value of first byte of key > 126 then assign that key to partition noOfPartitions-1
		 * 3. ASCII value of first byte of key >=32 and <= 126 then assign that key to proper partition
		 */
		if(noOfPartitions == 0 || noOfPartitions == 1 || key.charAt(0) < 32)
			return 0;
		else if (key.charAt(0) >126){
			return noOfPartitions -1;
		}else{
			int total = 95;
			int part = ((int)Math.ceil(total/noOfPartitions))+1;
			int partitionNumber  = (int)Math.floor((key.charAt(0)-32)/part);
			return partitionNumber;
		}
	}

}
