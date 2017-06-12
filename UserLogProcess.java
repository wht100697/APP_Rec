
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UserLogProcess {
	static class AveMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text mapOutKey = new Text();
		private Text mapOutValue = new Text();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String lineValue = value.toString();
			String strs[] = lineValue.split(",");
			mapOutKey.set(strs[0] + '\t' + strs[1]);
			mapOutValue.set(strs[2]);
			context.write(mapOutKey, mapOutValue);
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			super.cleanup(context);
		}
	}

	static class AveReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
		}

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String temp = "";
			int view = 0, collect = 0, download = 0;
			for (Text value : values) {
				temp = value.toString();
				if (temp.equals("1")) {
					view++;
				} else if (temp.equals("2")) {
					collect++;
				} else if (temp.equals("3")) {
					download++;
				}
			}
			temp = String.valueOf(view) + "\t" + String.valueOf(collect) + "\t" + String.valueOf(download);
			context.write(new Text(key), new Text(temp));
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			super.cleanup(context);
		}
	}

	public int run(String args[]) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, UserLogProcess.class.getSimpleName());
		job.setJarByClass(UserLogProcess.class);
		Path inPutDir = new Path(args[0]);
		FileInputFormat.addInputPath(job, inPutDir);
		job.setMapperClass(AveMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(AveReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		Path outPutDir = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outPutDir);
		boolean isSuccess = job.waitForCompletion(true);
		return isSuccess ? 0 : 1;
	}

	public static void main(String args[]) throws Exception {
		args = new String[] { "hdfs://NameNode:9000/input2", "hdfs://NameNode:9000/output2" };
		int status = new UserLogProcess().run(args);
		System.exit(status);
	}
}
