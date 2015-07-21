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
package de.bankmark.bigbench.queries.q03;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.hadoop.hive.contrib.mr.GenericMR;
import org.apache.hadoop.hive.contrib.mr.Output;
import org.apache.hadoop.hive.contrib.mr.Reducer;

/**
 * Currently not used by q3. q3 is using python
 * 
 * @author Michael Frank <michael.frank@bankmark.de>
 * @version 1.0 07.05.2014
 */
public class Red {

	private static final boolean TEST = false;

	public static void main(String[] args) throws Exception {

		int q03_days_before_purchase = Integer.parseInt(args[0]);
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

		doGenericMR(in, out, q03_days_before_purchase);

		if (TEST) {
			System.out.println("\nOUTPUT:");
			System.out.println(new String(((ByteArrayOutputStream) out)
					.toByteArray()));
		}

	}

	private static String makeTestData() {
		return "1\t2\t3\t4\t\\N\n" + "1\t2\t3\t4\t5\n" + "1\t2\t3\t4\t5\n";
	}

	public static class StringArrayComparator implements Comparator<String[]> {
		public static final StringArrayComparator INSTANCE = new StringArrayComparator();

		@Override
		public int compare(String[] o1, String[] o2) {
			return o1[0].compareTo(o2[0]);
		}
	}

	private static void doGenericMR(InputStream in, OutputStream out,
			final int q03_days_before_purchaseParam) throws Exception {
		new GenericMR().reduce(in, out, new Reducer() {
			@Override
			public void reduce(String key, Iterator<String[]> vals, Output out)
					throws Exception {
				final int q03_days_before_purchase = q03_days_before_purchaseParam;

				ArrayList<String[]> list = new ArrayList<String[]>();
				while (vals.hasNext()) {
					String[] tmp = vals.next(); // []={user, wcs_date, wcs_time,
												// item_key, sale}

					if (tmp.length != 5) {
						throw new IllegalArgumentException(
								"Arguments mismatch. Expected 5 {user, wcs_date, wcs_time, item_key, sale} but was:"
										+ Arrays.toString(tmp));
					}
					// cut user (served as grouping key), we no longer need it
					String[] tmp2 = new String[4];
					System.arraycopy(tmp, 1, tmp2, 0, 4);

					list.add(tmp2);
				}

				Collections.sort(list, StringArrayComparator.INSTANCE);

				String last_viewed_item = null;
				int last_viewed_date = -1;
				for (String[] i : list) {

					// i[0]==wcs_date
					// i[1]==wcs_time
					// i[2]==item_key
					// i[3]==sale
					String item_key = i[2];
					boolean itemKeyEmptyOrNull = isEmptyOrNull(item_key);
					boolean saleEmpyOrNull = isEmptyOrNull(i[3]);

					if (saleEmpyOrNull && !itemKeyEmptyOrNull) {

						try {
							last_viewed_date = Integer.parseInt(i[0]);
							last_viewed_item = item_key;
						} catch (NumberFormatException e) {
							// ignore bad line
							continue;
						}
					} else if (!saleEmpyOrNull && !itemKeyEmptyOrNull
							&& last_viewed_item != null) {
						try {
							int i0 = Integer.parseInt(i[0]);
							// -- AND purchased_date-lastviewed_date < 11
							if (last_viewed_date >= (i0 - q03_days_before_purchase)) {
								out.collect(new String[] { last_viewed_item,
										item_key });
								last_viewed_item = item_key;
								last_viewed_date = i0;
							}
						} catch (NumberFormatException e) {
							// ignore bad line
							continue;
						}

					}
				}

			}
		});
	}

	private static final boolean isEmptyOrNull(String s) {
		return s == null || s.isEmpty() || s.equals("\\N");
	}
}
