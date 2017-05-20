package dictionary;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author Kevin & Emil
 *	org.apache.hadoop.mapreduce.Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
 */
public class DictionaryReducer extends Reducer<Text, Text,Text, Text> {

	@Override 
	public void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException { 

		// Source: http://stackoverflow.com/questions/12899953/in-java-how-to-append-a-string-more-efficiently
		// Build the end string, containing the english word as key, and the other non-english words as value
		StringBuilder s = new StringBuilder();
		for(Text t : values){
			s.append("|" + t.toString().replace(',', '|'));
		}
		context.write(key,new Text(s.toString()));
	}
}