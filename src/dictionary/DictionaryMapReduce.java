package dictionary;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/**
 * 
 * @author Kevin & Emil
 *	Lab 11 Dictionary Task
 *	To start this program: "Run configuration" with the following parameter:
 *	languageFile1 languageFile2 languageFile3 ... languageFileX [outPutFolder]
 *	French.txt Italian.txt German.txt outputDictionary
 */

public class DictionaryMapReduce extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new DictionaryMapReduce(), args);
		System.exit(res);
	}

	/**
	 * Check if a valid args list was set.
	 */
	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 3) { 
			System.err.printf("Usage: %s [generic options] <in> <out>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		Job job = Job.getInstance(getConf(), "dictionaryMapReduce");
		job.setJarByClass(this.getClass());
	    job.setJobName("DictionaryMapReduce Driver");

	    // From the Hinweis at page 3: Hinweis Das Input Format für den Mapper ist als KeyValueTextInputFormat definiert. Damit
	    // kann eine Map Funktion eine Datei, deren Zeilen aus Key-Value Paaren besteht (durch TAB getrennt) direkt lesen.
		job.setInputFormatClass(KeyValueTextInputFormat.class);

		// Setting my own DictionaryMapper
		job.setMapperClass(DictionaryMapper.class);
		
		// Key --> Englisch Word: Text
		job.setMapOutputKeyClass(Text.class);
		// Value --> Non English Word: Text
		job.setMapOutputValueClass(Text.class);
		job.setPartitionerClass(HashPartitioner.class);

		// Setting my own DictionaryReducer
		job.setReducerClass(DictionaryReducer.class);
		// Key --> Englisch Word: Text
		job.setOutputKeyClass(Text.class);
		// Value --> Non English Word: Text
		job.setOutputValueClass(Text.class);

		job.setOutputFormatClass(TextOutputFormat.class);

		// Since this programs takes N arguments: Take args0 .... argsN-1 as InputFiles
		for(int i = 0; i < args.length-1; i++){
			FileInputFormat.addInputPath(job, new Path(args[i]));
		}
		// Last args string is the name for the output dir
		FileOutputFormat.setOutputPath(job, new Path(args[args.length-1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
