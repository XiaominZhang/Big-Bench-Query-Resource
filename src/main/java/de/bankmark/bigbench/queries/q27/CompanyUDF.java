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

package de.bankmark.bigbench.queries.q27;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import opennlp.tools.util.Span;

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
public class CompanyUDF extends GenericUDTF {

	// objects created in default constructor() . Dont serialize!
	private transient SentenceDetectorME sentenceDetector;
	private transient Tokenizer tokenizer;
	private transient NameFinderME nameFinder;

	// objects created in initialize()
	private transient PrimitiveObjectInspector stringOI = null;
	private transient PrimitiveObjectInspector longOI = null;
	private transient PrimitiveObjectInspector longOI2 = null;

	public CompanyUDF() throws IOException {
		System.out.println("new " + this.getClass().getName() + "()");
		InputStream sentenceModelIn = null;
		InputStream tokenModelIn = null;
		InputStream nameModelIn = null;

		try {
			sentenceModelIn = de.bankmark.bigbench.queries.PackageLocator
					.getPackageResourceAsStream("en-sent.bin");
			SentenceModel sentenceModel = new SentenceModel(sentenceModelIn);
			sentenceDetector = new SentenceDetectorME(sentenceModel);

			tokenModelIn = de.bankmark.bigbench.queries.PackageLocator
					.getPackageResourceAsStream("en-token.bin");
			TokenizerModel tokenModel = new TokenizerModel(tokenModelIn);
			tokenizer = new TokenizerME(tokenModel);

			nameModelIn = de.bankmark.bigbench.queries.PackageLocator
					.getPackageResourceAsStream("en-ner-organization.bin");
			TokenNameFinderModel nameModel = new TokenNameFinderModel(
					nameModelIn);

			nameFinder = new NameFinderME(nameModel);

		} catch (IOException ex) {
			ex.printStackTrace();
			throw ex;
		} finally {
			silentCloseStream(sentenceModelIn);
			silentCloseStream(tokenModelIn);
			silentCloseStream(nameModelIn);
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

	@Override
	public StructObjectInspector initialize(ObjectInspector[] args)
			throws UDFArgumentException {
		System.out.println("initialize() " + this.getClass().getName() + "");

		if (args.length != 3) {
			throw new UDFArgumentException(
					"find_company() takes exactly three argument: {long, long, string} but was: "
							+ args.length + "\n" + args);
		}

		if (args[0].getCategory() != ObjectInspector.Category.PRIMITIVE
				&& ((PrimitiveObjectInspector) args[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.LONG) {
			throw new UDFArgumentException(
					"find_company() takes a long as first parameter");
		}
		if (args[1].getCategory() != ObjectInspector.Category.PRIMITIVE
				&& ((PrimitiveObjectInspector) args[1]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.LONG) {
			throw new UDFArgumentException(
					"find_company() takes a long as second parameter");
		}
		if (args[2].getCategory() != ObjectInspector.Category.PRIMITIVE
				&& ((PrimitiveObjectInspector) args[2]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
			throw new UDFArgumentException(
					"find_company() takes a string as second parameter");
		}

		longOI = (PrimitiveObjectInspector) args[0];
		longOI2 = (PrimitiveObjectInspector) args[1];
		stringOI = (PrimitiveObjectInspector) args[2];

		List<String> fieldNames = new ArrayList<String>(4);
		List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(4);
		fieldNames.add("pr_sk");
		fieldNames.add("item_sk");
		fieldNames.add("company_name");
		fieldNames.add("review_sentence");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		StandardStructObjectInspector result = ObjectInspectorFactory
				.getStandardStructObjectInspector(fieldNames, fieldOIs);
		System.out.println("initialize() " + this.getClass().getName()
				+ " done");
		return result;
	}

	@Override
	public void process(Object[] record) throws HiveException {

		long pr_sk = (Long) longOI.getPrimitiveJavaObject(record[0]);
		long item_sk = (Long) longOI2.getPrimitiveJavaObject(record[1]);
		String review = (String) stringOI.getPrimitiveJavaObject(record[2]);
		if (review == null || review.isEmpty()) {
			return; // skip bad record
		}

		String[] sentences = sentenceDetector.sentDetect(review);
		for (int i = 0; i < sentences.length; i++) {
			String sentence = sentences[i];
			String[] tokens = tokenizer.tokenize(sentence);
			String[] names = Span.spansToStrings(nameFinder.find(tokens),
					tokens);
			for (String name : names) {
				forward(new Object[] { pr_sk, item_sk, name, sentence });
			}
			nameFinder.clearAdaptiveData();
		}
	}

	@Override
	public void close() throws HiveException {
	}

}
