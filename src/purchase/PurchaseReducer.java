package purchase;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * 
 * @author Kevin & Emil
 *	org.apache.hadoop.mapreduce.Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
 */
public class PurchaseReducer extends Reducer<Text, DoubleWritable,Text, DoubleWritable> {

	@Override 
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
			throws IOException, InterruptedException { 

		Double sum = 0.0d;
		// Add the current sum to the whole sum of previous calculation
		for (DoubleWritable val : values) { 
			sum += val.get(); 
		} 
		context.write(key,new DoubleWritable(sum));
	}
}