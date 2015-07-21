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
package de.bankmark.bigbench.queries.q29;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;

/**
 * Currently not used by q29. q29 is using python Category affinity analysis.
 * Step 1: aggregate category_id and category name for each "KEY" (e.g. orderID)
 * Step2: Compute distinct cross product for accumulated categories
 * 
 * @author Michael Frank <michael.frank@bankmark.de>
 * @version 1.0 08.07.2014
 */
public class Red {
	private static final boolean TEST = true;
	public static final char SEPARATOR = '\t';

	private BufferedWriter out;
	private BufferedReader in;

	private String oldKey = "";
	private ArrayList<Pair> vals = new ArrayList<Pair>();

	public Red(InputStream in, OutputStream out) {
		this.in = new BufferedReader(new InputStreamReader(in));
		this.out = new BufferedWriter(new OutputStreamWriter(out));

	}

	public static void main(final String[] args) throws Exception {
		try {
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

			new Red(in, out).startCustomReducer();

			if (TEST) {
				System.out.println("\nOUTPUT:");
				System.out.println(new String(((ByteArrayOutputStream) out)
						.toByteArray()));
			}
		} catch (Exception e) {
			FileWriter fw = new FileWriter("/tmp/q30reducer_error.log");
			fw.write(e.toString());
			fw.close();
			e.printStackTrace();
		}

	}

	private void startCustomReducer() throws IOException {
		try {
			BufferedReader in = this.in;
			String line;

			while ((line = in.readLine()) != null) {
				customReducerDoLine(line);
			}
			// finish last batch of accumulated values
			customReduce();
		} finally {
			out.close();
		}

	}

	private void customReducerDoLine(String line) throws IOException {
		line = line.trim();
		int i = line.indexOf(SEPARATOR);
		if (i < 0) {
			return; // skip bad line
		}
		String key = line.substring(0, i);
		String value = line.substring(i + 1, line.length());

		// aggregate values of same keys into list. (requires pre-sort/groupBy
		// from hive)
		if (key.equals(oldKey)) {
			add(value);
		} else {
			customReduce(); // flush accumulated values list
			oldKey = key;
			vals = null;
			vals = new ArrayList<Pair>();
			add(value);
		}
	}

	private void add(String line) {
		int i = line.indexOf(SEPARATOR);
		if (i > 0) {
			String catID = line.substring(0, i);
			String cat = line.substring(i + 1, line.length());
			try {
				long catID_long = Long.parseLong(catID);
				vals.add(new Pair(catID_long, cat));
			} catch (NumberFormatException e) {
				// skip bad value
			}
		}
	}

	private void customReduce() throws IOException {
		ArrayList<Pair> vals = this.vals;
		Collections.sort(vals);
		ArrayList<Pair> distinct = new ArrayList<Pair>();
		long lastID = -1;
		// write distinct combination of pairs
		for (Pair i : vals) {
			if (lastID != i.id) {
				lastID = i.id;
				for (Pair j : distinct) {
					writeLine(i, j);
					writeLine(j, i);
				}
				distinct.add(i);
			}
		}
	}

	/**
	 * Streaming api fileformat writer
	 * 
	 * @param i
	 * @param j
	 * @throws IOException
	 */
	private void writeLine(Pair i, Pair j) throws IOException {
		out.write(Long.toString(i.id));
		out.write(SEPARATOR);
		out.write(i.name);
		out.write(SEPARATOR);
		out.write(Long.toString(j.id));
		out.write(SEPARATOR);
		out.write(j.name);
		out.write('\n');
	}

	private static String makeTestData() {
		StringBuilder sb = new StringBuilder();
		String[][] test = { { "1", "foo" }, { "1", "foo" }, { "3", "bar" },
				{ "3", "bar" }, { "30", "foobar" }, { "5", "alice" } };
		String[] keys = { "a", "b", "c" };
		for (String key : keys) {
			for (int i = 0; i < test.length; i++) {
				sb.append(key);
				sb.append(SEPARATOR);
				sb.append(test[i][0]);
				sb.append(SEPARATOR);
				sb.append(test[i][1]);
				sb.append("\n");
			}
		}
		return sb.toString();
	}

	static class Pair implements Comparable<Pair> {
		public Pair(long id, String name) {
			super();
			this.id = id;
			this.name = name;
		}

		long id;
		String name;

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof Pair)
				return id == ((Pair) obj).id;
			return false;
		}

		@Override
		public int compareTo(Pair o) {
			return Long.compare(id, o.id);
		}
	}
}
