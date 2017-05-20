package purchase;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * @author Kevin & Emil
 *	Lab 11 Purchase Task
 *	To start this program: "Run configuration" with the following parameter:
 *	purchases.txt [outputFolder]
 */
public class PurchaseMapReduce extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new PurchaseMapReduce(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <in> <out>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		Job job = Job.getInstance(getConf(), "purchaseMapReduce");
		job.setJarByClass(this.getClass());
	    job.setJobName("PurchaseMapReduce Driver");

	    // TextInput since the id is a Long 
		job.setInputFormatClass(TextInputFormat.class);
		// Setting own mapper
		job.setMapperClass(PurchaseMapper.class);
		// Key--> The Item/category: Text
		job.setMapOutputKeyClass(Text.class);
		// Value --> Number of sold items
		job.setMapOutputValueClass(DoubleWritable.class);

		job.setPartitionerClass(HashPartitioner.class);
		
		//Setting own Reducer
		job.setReducerClass(PurchaseReducer.class);
		// Key--> The Item/category: Text
		job.setOutputKeyClass(Text.class);
		// Value --> Number of sold items
		job.setOutputValueClass(DoubleWritable.class);

		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
