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

package de.bankmark.bigbench.queries.q18;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Michael Frank <michael.frank@bankmark.de>
 * @version 1.0 25.04.2014
 */
public class MRlinearRegression extends Configured implements Tool {

	static class LRmapper extends
			Mapper<LongWritable, Text, IntWritable, DoubleArrayWritable> {

		// cached writeables
		private static final IntWritable KEY_ONE = new IntWritable(1);
		private DoubleWritable dw_x = new DoubleWritable();
		private DoubleWritable dw_y = new DoubleWritable();
		private DoubleWritable dw_xy = new DoubleWritable();
		private DoubleWritable dw_xx = new DoubleWritable();
		private DoubleArrayWritable array = new DoubleArrayWritable(
				new DoubleWritable[] { dw_x, dw_y, dw_xy, dw_xx });

		public void map(
				LongWritable key,
				Text values,
				Mapper<LongWritable, Text, IntWritable, DoubleArrayWritable>.Context context)
				throws IOException, InterruptedException {

			String a = values.toString();
			int i = a.indexOf(' ');
			String x_str = a.substring(0, i);
			String y_str = a.substring(i + 1, a.length());

			double x = Double.parseDouble(x_str);
			double y = Double.parseDouble(y_str);
			double xy = x * y;
			double xSquared = sq(x);

			dw_x.set(x);
			dw_y.set(y);
			dw_xy.set(xy);
			dw_xx.set(xSquared);

			context.write(KEY_ONE, array);
		}
	}

	static class LRreducer
			extends
			Reducer<IntWritable, DoubleArrayWritable, DoubleWritable, DoubleWritable> {

		// cached writeables
		private DoubleWritable dw_intercept = new DoubleWritable();
		private DoubleWritable dw_slope = new DoubleWritable();

		public void reduce(
				IntWritable key,
				Iterable<DoubleArrayWritable> valueArray,
				Reducer<IntWritable, DoubleArrayWritable, DoubleWritable, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			int N = 0;
			double sumX = 0.0D;
			double sumY = 0.0D;
			double sumXY = 0.0D;
			double sumXSquared = 0.0D;
			for (DoubleArrayWritable value : valueArray) {
				Writable[] values = value.get();
				sumX += ((DoubleWritable) values[0]).get();
				sumY += ((DoubleWritable) values[1]).get();
				sumXY += ((DoubleWritable) values[2]).get();
				sumXSquared += ((DoubleWritable) values[3]).get();
				N++;
			}
			double numerator = N * sumXY - sumX * sumY;
			double denom = N * sumXSquared - (sq(sumX));

			double slope = numerator / denom;
			double intercept = (sumY - slope * sumX) / N;
			dw_intercept.set(intercept);
			dw_slope.set(slope);
			context.write(dw_intercept, dw_slope);
		}
	}

	public static double sq(double x) {
		return x * x;
	}

	@Override
	public int run(String[] args) throws Exception {

		int NUMBER_REDUCERS = 1;
		Job job = Job.getInstance(getConf());

		job.setJarByClass(MRlinearRegression.class);
		if (args.length != 2) {
			usage(job);
			return 2;
		}
		System.out.println("input:");
		job.setJobName(MRlinearRegression.class.getSimpleName() + "::"
				+ args[0] + "->" + args[1]);

		Path input = new Path(args[0]);
		Path output = new Path(args[1]);
		System.out.println("Input: " + input + "  out -> " + output);
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);

		job.setMapperClass(MRlinearRegression.LRmapper.class);
		job.setReducerClass(MRlinearRegression.LRreducer.class);
		job.setNumReduceTasks(NUMBER_REDUCERS);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(DoubleArrayWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	private void usage(Job job) {
		String jar = job.getJar() != null ? job.getJar() : " jarName.jar ";
		System.err
				.println("Usage:\n hadoop jar "
						+ jar
						+ " "
						+ this.getClass().getName()
						+ " <inDir> <outDir> \n<inDir> file format: <double x> <double y> \n<outDir> file format: <double intercept> <double slope>");
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new MRlinearRegression(),
				args);
		System.exit(res);
	}
}
