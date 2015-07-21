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
package de.bankmark.bigbench.queries.q08;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.hive.contrib.mr.GenericMR;
import org.apache.hadoop.hive.contrib.mr.Output;
import org.apache.hadoop.hive.contrib.mr.Reducer;

/**
 * @author Michael Frank <michael.frank@bankmark.de>
 * @version 1.0 07.05.2014
 */
public class Red {

	private static final boolean TEST = false;

	public static void main(String[] args) throws Exception {
		String category = args[0];
		InputStream in;
		OutputStream out;
		if (TEST) {
			String testData = makeTestData();
			System.out.println("INPUT DATA\n=========");
			System.out.println(testData);

			out = new ByteArrayOutputStream();
			in = new ByteArrayInputStream(testData.getBytes());

		} else {
			in = System.in;
			out = System.out;
		}

		doGenericMR(in, out, category);

		if (TEST) {
			System.out.println("\nOUTPUT:");
			System.out.println(new String(((ByteArrayOutputStream) out)
					.toByteArray()));
		}

	}

	private static String makeTestData() {
		// TODO Auto-generated method stub
		return "1	26133	12311	171574	ad\r\n"
				+ "1	26133	12311	171574	welcome\r\n"
				+ "1	26133	12311	171574	dynamic\r\n"
				+ "1	26334	12688	\\N	dynamic\r\n"
				+ "1	26334	31553	\\N	dynamic\r\n"
				+ "1	26334	62567	\\N	dynamic\r\n" + "1	26334	2043	\\N	ad\r\n"
				+ "1	26133	12311	171574	welcome\r\n"
				+ "1	26133	12311	171574	ad\r\n"
				+ "1	26334	70873	\\N	general\r\n"
				+ "1	26334	55876	\\N	feedback\r\n" + "1	28182	22909	\\N	ad\r\n"
				+ "1	28182	24136	\\N	ad\r\n"
				+ "1	26133	12311	171574	feedback\r\n"
				+ "1	28182	62991	\\N	feedback\r\n"
				+ "1	26334	49758	\\N	dynamic\r\n"
				+ "1	28182	1905	\\N	welcome\r\n"
				+ "1	26133	12311	171574	welcome\r\n"
				+ "1	26334	11142	\\N	general\r\n"
				+ "1	28182	12868	\\N	feedback\r\n"
				+ "1	28182	40192	\\N	general\r\n"
				+ "1	28182	3887	\\N	general\r\n"
				+ "1	26334	52545	\\N	feedback\r\n"
				+ "1	26334	3842	\\N	dynamic\r\n"
				+ "1	26334	66307	\\N	order\r\n"
				+ "1	28182	7860	\\N	general\r\n"
				+ "1	26133	12311	171574	ad\r\n"
				+ "1	26133	12311	171574	welcome\r\n"
				+ "1	28182	18414	\\N	general\r\n"
				+ "1	28182	84880	\\N	protected\r\n"
				+ "1	28182	7669	\\N	protected\r\n"
				+ "1	26334	84201	\\N	welcome\r\n" + "";
	}

	private static class Q8ReducerEntry implements Comparable<Q8ReducerEntry> {
		long wcs_date;
		long wcs_time;
		String sales_sk;
		String wpt;

		public Q8ReducerEntry(String[] tmp) throws NumberFormatException {
			// tmp [] ={uid, c_date, c_time, sales_sk, wpt}
			wcs_date = Long.parseLong(tmp[1]);
			wcs_time = Long.parseLong(tmp[2]);
			sales_sk = tmp[3];
			wpt = tmp[4];
		}

		@Override
		public int compareTo(Q8ReducerEntry o) {

			int i = Long.compare(wcs_date, o.wcs_date);
			if (i != 0)
				return i;
			return Long.compare(wcs_time, o.wcs_time);

		}

	}

	private static void doGenericMR(InputStream in, OutputStream out,
			final String categoryParam) throws Exception {
		new GenericMR().reduce(in, out, new Reducer() {

			@Override
			public void reduce(String key, Iterator<String[]> vals, Output out)
					throws Exception {
				final String category = categoryParam;
				ArrayList<Q8ReducerEntry> list = new ArrayList<Q8ReducerEntry>();
				while (vals.hasNext()) {
					String[] tmp = vals.next(); // []={uid,uid, c_date, c_time,
												// sales_sk, wpt}

					if (tmp.length != 5) {
						throw new IllegalArgumentException(
								"Arguments mismatch. Expected 5 {uid, c_date, c_time, sales_sk, wpt} but was:"
										+ Arrays.toString(tmp));
					}
					// cut uid (served as grouping key), we no longer need it
					try {
						list.add(new Q8ReducerEntry(tmp));
					} catch (NumberFormatException e) {

					}
				}

				Collections.sort(list);

				int ready = 0;
				for (Q8ReducerEntry entry : list) {
					if (ready == 0 && entry.wpt.equals(category)) {
						ready = 1;
						continue;
					}
					if (ready == 1 && !isEmptyOrNull(entry.sales_sk)) {
						out.collect(new String[] {
								Long.toString(entry.wcs_date), entry.sales_sk });
					}

				}
			}
		});
	}

	private static final boolean isEmptyOrNull(String s) {
		return s == null || s.isEmpty() || s.equals("\\N");
	}
}
