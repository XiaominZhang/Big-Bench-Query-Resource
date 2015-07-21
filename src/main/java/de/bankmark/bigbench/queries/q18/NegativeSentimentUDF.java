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
public class NegativeSentimentUDF extends GenericUDTF {
	private transient static final String POS = "POS";
	private transient static final String NEG = "NEG";

	// objects created in default constructor() . Dont serialize!
	private transient HashSet<String> positiveTokens = null;
	private transient HashSet<String> negativeTokens = null;
	private transient SentenceDetectorME sentenceDetector = null;
	private transient Tokenizer tokenizer = null;

	// objects created in initialize()

	private transient PrimitiveObjectInspector store_OI;
	private transient PrimitiveObjectInspector review_date_OI;
	private transient PrimitiveObjectInspector review_OI;

	public NegativeSentimentUDF() {
		System.out.println("new " + this.getClass().getName() + "()");
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

			positiveTokens = readTokens(de.bankmark.bigbench.queries.q10.PackageLocator
					.getPackageResourceAsStream("positiveSentiment.txt"));
			negativeTokens = readTokens(de.bankmark.bigbench.queries.q10.PackageLocator
					.getPackageResourceAsStream("negativeSentiment.txt"));
		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			silentCloseStream(sentenceModelIn);
			silentCloseStream(tokenModelIn);
		}
		System.out.println("new " + this.getClass().getName() + "() done");
	}

	private void silentCloseStream(InputStream stream) {
		if (stream != null) {
			try {
				stream.close();
			} catch (IOException e) {
				// e.printStackTrace();
			}
		}
	}

	private static HashSet<String> readTokens(InputStream in)
			throws IOException {
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
		System.out.println("initialize " + this.getClass().getName());
		if (args.length != 3) {
			throw new UDFArgumentException(
					"sentiment() takes exactly 3 argument: {string, string, string} but was: "
							+ args.length + "\n" + args);
		}

		if (args[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
			throw new UDFArgumentException(
					"sentiments() takes a string java object as first parameter");
		}

		if (args[1].getCategory() != ObjectInspector.Category.PRIMITIVE) {
			throw new UDFArgumentException(
					"sentiments() takes a string java object as second parameter");
		}

		if (args[2].getCategory() != ObjectInspector.Category.PRIMITIVE
				&& ((PrimitiveObjectInspector) args[2]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
			throw new UDFArgumentException(
					"sentiments() takes a string as third parameter");
		}

		store_OI = (PrimitiveObjectInspector) args[0];
		review_date_OI = (PrimitiveObjectInspector) args[1];
		review_OI = (PrimitiveObjectInspector) args[2];

		List<String> fieldNames = new ArrayList<String>(5);
		List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(5);
		fieldNames.add("store");
		fieldNames.add("review_date");
		fieldNames.add("sentence");
		fieldNames.add("sentiment");
		fieldNames.add("sentiment_word");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		StandardStructObjectInspector result = ObjectInspectorFactory
				.getStandardStructObjectInspector(fieldNames, fieldOIs);
		System.out.println("initialize " + this.getClass().getName() + " done");
		return result;
	}

	@Override
	public void close() throws HiveException {
	}

	@Override
	public void process(Object[] record) throws HiveException {

		String review = (String) review_OI.getPrimitiveJavaObject(record[2]);

		if (review == null || review.isEmpty()) {
			return; // skip bad records
		}

		String[] sentences = sentenceDetector.sentDetect(review);
		for (int i = 0; i < sentences.length; i++) {
			String sentence = sentences[i];
			String[] tokens = tokenizer.tokenize(sentence);
			for (int j = 0; j < tokens.length; j++) {
				String token = tokens[j];

				// if (positiveTokens.contains(token)) {
				// // Positive sentiment
				// forward(new Object[] { record[0], record[1], sentence, POS,
				// token });
				// } else
				if (negativeTokens.contains(tokens[j])) {
					// Negative sentiment
					forward(new Object[] { record[0], record[1], sentence, NEG,
							token });
				}
			}
		}

	}

	public static void main(String[] args) throws InvalidFormatException,
			IOException {

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

		NegativeSentimentUDF udf = new NegativeSentimentUDF();
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