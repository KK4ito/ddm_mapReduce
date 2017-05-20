package dictionary;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author Kevin & Emil
 *	org.apache.hadoop.mapreduce.Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
 */
public class DictionaryMapper extends Mapper<Text, Text, Text, Text> {

	@Override
	public void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		
		// Skip comments at the beginning
		if(key.charAt(0) != '#') {
			context.write(key, value);
		}
		
	}
}