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
package de.bankmark.bigbench.queries.q10;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import opennlp.tools.util.InvalidFormatException;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * This class implements a Hive UDF for sentiment analysis in the BigBench big
 * data analytics benchmark. The analysis performs a dictionary lookup to
 * extract positive or negative sentiments in sentences.
 * 
 * @author Tilmann Rabl <tilmann.rabl@bankmark.de>
 * @author Michael Frank <michael.frank@bankmark.de>
 */
public class SentimentUDF extends GenericUDTF {

	private transient static final String POS = "POS";
	private transient static final String NEG = "NEG";

	// objects created in default constructor() . Dont serialize!
	private transient HashSet<String> positiveTokens = null;
	private transient HashSet<String> negativeTokens = null;
	private transient SentenceDetectorME sentenceDetector = null;
	private transient Tokenizer tokenizer = null;

	// objects created in initialize()
	private transient PrimitiveObjectInspector stringOI = null;
	private transient PrimitiveObjectInspector longOI = null;

	public SentimentUDF() {
		System.out.println("initialize " + this.getClass().getName() + "()");
		InputStream sentenceModelIn = null;
		InputStream tokenModelIn = null;

		try {
			// sentenceModelIn = new FileInputStream("en-sent.bin");
			sentenceModelIn = de.bankmark.bigbench.queries.PackageLocator
					.getPackageResourceAsStream("en-sent.bin");
			SentenceModel sentenceModel = new SentenceModel(sentenceModelIn);
			sentenceDetector = new SentenceDetectorME(sentenceModel);

			// tokenModelIn = new FileInputStream("en-token.bin");
			tokenModelIn = de.bankmark.bigbench.queries.PackageLocator
					.getPackageResourceAsStream("en-token.bin");
			TokenizerModel tokenModel = new TokenizerModel(tokenModelIn);
			tokenizer = new TokenizerME(tokenModel);

			positiveTokens = readTokens("positiveSentiment.txt");
			negativeTokens = readTokens("negativeSentiment.txt");
		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			silentCloseStream(sentenceModelIn);
			silentCloseStream(tokenModelIn);
		}
		System.out.println("initialize " + this.getClass().getName()
				+ "() done");
	}

	private void silentCloseStream(InputStream stream) {
		if (stream != null) {
			try {
				stream.close();
			} catch (IOException e) {
			}
		}
	}

	private static HashSet<String> readTokens(String filename)
			throws IOException {
		InputStream in = SentimentUDF.class.getResourceAsStream(filename);
		// InputStream in = new FileInputStream(filename);
		BufferedReader bufferedReader = new BufferedReader(
				new InputStreamReader(in));
		HashSet<String> set = new HashSet<String>();
		String line = null;
		while ((line = bufferedReader.readLine()) != null) {
			set.add(line);
		}
		bufferedReader.close();
		return set;
	}

	/**
	 * Initializes the Q10UDF Builds the models and loads
	 */
	public StructObjectInspector initialize(ObjectInspector[] args)
			throws UDFArgumentException {
		System.out.println("initialize() " + this.getClass().getName());

		if (args.length != 2) {
			throw new UDFArgumentException(
					"sentiment() takes exactly 2 argument: {long, string} but was: "
							+ args.length + "\n" + args);
		}

		if (args[0].getCategory() != ObjectInspector.Category.PRIMITIVE
				&& ((PrimitiveObjectInspector) args[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.LONG) {
			throw new UDFArgumentException(
					"sentiments() takes a long as first parameter");
		}

		if (args[1].getCategory() != ObjectInspector.Category.PRIMITIVE
				&& ((PrimitiveObjectInspector) args[1]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
			throw new UDFArgumentException(
					"sentiments() takes a string as second parameter");
		}

		longOI = (PrimitiveObjectInspector) args[0];
		stringOI = (PrimitiveObjectInspector) args[1];

		List<String> fieldNames = new ArrayList<String>(4);
		List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(4);
		fieldNames.add("item_sk");
		fieldNames.add("sentence");
		fieldNames.add("sentiment");
		fieldNames.add("sentiment_word");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		StandardStructObjectInspector result = ObjectInspectorFactory
				.getStandardStructObjectInspector(fieldNames, fieldOIs);
		System.out.println("initialize() " + this.getClass().getName()
				+ " done");
		return result;
	}

	@Override
	public void close() throws HiveException {
	}

	@Override
	public void process(Object[] record) throws HiveException {

		long item_sk = (Long) longOI.getPrimitiveJavaObject(record[0]);
		String review = (String) stringOI.getPrimitiveJavaObject(record[1]);

		if (review == null || review.isEmpty()) {
			return;
		}

		String[] sentences = sentenceDetector.sentDetect(review);
		for (int i = 0; i < sentences.length; i++) {
			String sentence = sentences[i];
			String[] tokens = tokenizer.tokenize(sentence);
			for (int j = 0; j < tokens.length; j++) {
				String token = tokens[j];

				if (positiveTokens.contains(token)) {
					forward(new Object[] { item_sk, sentence, POS, token });
					// forward(new Object[] {review,POS,tokens[j]});
				} else if (negativeTokens.contains(tokens[j])) {
					forward(new Object[] { item_sk, sentence, NEG, token });
				}
			}
		}
	}

	public static void main(String[] args) throws InvalidFormatException,
			IOException, HiveException {

		// test();

	}

	private static void test() {
		String review = new String(
				"For the most part any book by David as one might expect "
						+ "from having so much that am struggling with sixteenth-century texts at "
						+ "the author's faith in the vein of Richard N. Current's intelligent The Lincoln "
						+ "Nobody Knows. Had Donald accomplished nothing else like this available. "
						+ "One-hundred different models of writing gathered together in one place. "
						+ "Both researchers and the plethora of dilemmas such concept, recommend "
						+ "reading Barry Schwartz's THE PARADOX OF CHOICE: WHEN MORE IS LESS, for "
						+ "expanded development of their need to express herself reasserts itself at "
						+ "the end of the South--and the result was near disaster.\" (p. 13) Let us "
						+ "pray in 2013 that history does not repeat itself. In more or less "
						+ "chronological order Donald examines: Getting Right With Lincoln, The "
						+ "Folklore Lincoln, Toward Reconsideration of Abolitionists, An Excess "
						+ "of Democracy: The American Civil War at the Hotel Queen Mary in the "
						+ "first in Churchill's comprehensive History of the donors at The Body "
						+ "Bank. In STARCHILD The Collar is still some very valuable information that "
						+ "have not read anything threatening Maeve Binchy has in print.");

		SentimentUDF udf = new SentimentUDF();
		System.out.println(udf.positiveTokens);
		System.out.println(udf.negativeTokens);
		SentenceDetectorME sentenceDetector = udf.sentenceDetector;
		Tokenizer tokenizer = udf.tokenizer;

		String[] sentences = sentenceDetector.sentDetect(review);
		for (int i = 0; i < sentences.length; i++) {
			String[] tokens = tokenizer.tokenize(sentences[i]);
			// String[] tokens = tokenizer.tokenize(review);
			// System.out.println(Arrays.toString(tokens));
			for (int j = 0; j < tokens.length; j++) {
				if (udf.positiveTokens.contains(tokens[j])) {
					System.out.println(sentences[i] + "   " + POS + "   "
							+ tokens[j]);
				} else if (udf.negativeTokens.contains(tokens[j])) {
					System.out.println(sentences[i] + "   " + NEG + "   "
							+ tokens[j]);
				}
			}
		}
	}
}
