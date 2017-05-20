package purchase;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author Kevin & Emil
 *	org.apache.hadoop.mapreduce.Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
 */
public class PurchaseMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		Text item = new Text(); 
		DoubleWritable price = new DoubleWritable();

		//Split the read lines into key (item) and value (price) by TAB
		String[] rawSplitted = value.toString().split("\t+");
		item.set(rawSplitted[3]);	
		price.set(Double.parseDouble(rawSplitted[4]));
		context.write(item, price);
	}
}