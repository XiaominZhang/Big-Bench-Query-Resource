/*******************************************************************************
 * Copyright (c) 2014 bankmark UG 
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package de.bankmark.bigbench.queries.q28;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Converts hive output of q28 to the input format mahout expects.
 * 
 * One may argue that hive can natively store its tables as sequence files, but
 * hive ignores the "key" of a sequencefile and puts everything into the "value"
 * part.
 * 
 * From there you have two options, a) write a custom Serde for hive b) write a
 * conversion hadoop job (which is what we choose to do)
 * 
 * @author Michael Frank <michael.frank@bankmark.de>
 * @version 1.0 26.05.2014
 */
public class ToSequenceFile extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new ToSequenceFile(),
				args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {

		Job job = Job.getInstance(getConf());

		job.setJarByClass(ToSequenceFile.class);
		if (args.length != 2) {
			usage(job);
			return 2;
		}
		System.out.println("input:");
		job.setJobName(ToSequenceFile.class.getSimpleName() + "::" + args[0]
				+ "->" + args[1]);

		Path input = new Path(args[0]);
		Path output = new Path(args[1]);
		System.out.println("Input: " + input + "  out -> " + output);
		FileInputFormat.addInputPath(job, input);
		SequenceFileOutputFormat.setOutputPath(job, output);

		job.setMapperClass(IdentityMapper.class);
		job.setReducerClass(Reducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(0);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	private void usage(Job job) {
		String jar = job.getJar() != null ? job.getJar() : " jarName.jar ";
		System.err
				.println("Usage:\n hadoop jar "
						+ jar
						+ " "
						+ this.getClass().getName()
						+ " <inDir> <outDir> \n<inDir> file format: tab separated csv: <documentID> <text> \n<outDir> file format Sequencefile: <Text documentID> <Text content>");
	}

	public static class IdentityMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		private StringBuilder sb = new StringBuilder();
		private Text seq_key = new Text();
		private Text seq_value = new Text();

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			// value format: "<review_sk>\t<rating>\t<content>"

			// fields layout {review_sk, rating, content}
			String[] fields = value.toString().split("\t");
			if (fields.length >= 3) {
				StringBuilder sb = this.sb;

				// Transform:
				// SeqKey: "/<rating>/<review_sk>"
				// SeqValue: "<content>"
				// Mahout will automatically extract the stuff between: / / as
				// labels and categories for classification

				// make SeqKey:
				sb.setLength(0); // reset buffer
				sb.append('/').append(fields[1]).append('/').append(fields[0]);

				// write seq entry
				seq_key.set(sb.toString());
				seq_value.set(fields[2]);
				context.write(seq_key, seq_value);
			}

		}

	}

}
