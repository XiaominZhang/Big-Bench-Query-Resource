package de.bankmark.bigbench.queries.q01;

import opennlp.tools.util.InvalidFormatException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class transform2 extends GenericUDTF {

    static final Log LOG = LogFactory.getLog(transform2.class);

	// objects created in initialize()
	private transient PrimitiveObjectInspector stringOI = null;

	public transform2() {
	}

	private void silentCloseStream(InputStream stream) {
		if (stream != null) {
			try {
				stream.close();
			} catch (IOException e) {
			}
		}
	}

	/**
	 * Initializes the Q10UDF Builds the models and loads
	 */
	public StructObjectInspector initialize(ObjectInspector[] args)
			throws UDFArgumentException {

		if (args.length != 1) {
			throw new UDFArgumentException(
					"sentiment() takes exactly 1 argument: {string} but was: "
							+ args.length + "\n" + args);
		}

		if (args[0].getCategory() != ObjectInspector.Category.PRIMITIVE
				&& ((PrimitiveObjectInspector) args[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
			throw new UDFArgumentException(
					"sentiments() takes a long as first parameter");
		}

		stringOI = (PrimitiveObjectInspector) args[0];

		List<String> fieldNames = new ArrayList<String>(2);
		List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(2);
		fieldNames.add("pid1");
		fieldNames.add("pid2");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
		StandardStructObjectInspector result = ObjectInspectorFactory
				.getStandardStructObjectInspector(fieldNames, fieldOIs);
		return result;
	}

	@Override
	public void close() throws HiveException {
	}

	@Override
	public void process(Object[] record) throws HiveException {

		String item = (String) stringOI.getPrimitiveJavaObject(record[0]);

		if (item == null || item.isEmpty()) {
			return;
		}

        String[] vals = item.split(" ");
        ArrayList<String> tokens = new ArrayList<String>();
        for(String s : vals){
            tokens.add(s);
        }
        Collections.sort(tokens);
        boolean x=false;
        for (int i = 0; i < tokens.size()-1; i++) {
            //if(tokens.get(i).equals("1415"))
            //    x=true;
            StringBuilder sb = new StringBuilder();
            for (int j = i + 1; j < tokens.size(); j++) {
                //if(x) {
                //    sb.append(tokens.get(j));
                //    sb.append(" ");
                //}
                forward(new Object[]{Long.parseLong(tokens.get(i)), Long.parseLong(tokens.get(j))});
            }
            //if(x)
            //    LOG.error(sb.toString());
            //x=false;
            //LOG.error(tokens.get(i)+","+(tokens.size()-i));
        }
	}
}
