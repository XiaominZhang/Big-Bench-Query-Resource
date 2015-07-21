package de.bankmark.bigbench.queries.q30;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantIntObjectInspector;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class transform2 extends GenericUDTF {

    static final Log LOG = LogFactory.getLog(transform2.class);

	// objects created in initialize()
    private transient StandardListObjectInspector loi = null;

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

        loi = (StandardListObjectInspector) args[0];

		List<String> fieldNames = new ArrayList<String>(2);
		List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(2);
		fieldNames.add("category_id");
		fieldNames.add("category");
        fieldNames.add("affine_category_id");
        fieldNames.add("affine_category");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        StandardStructObjectInspector result = ObjectInspectorFactory
				.getStandardStructObjectInspector(fieldNames, fieldOIs);
		return result;
	}

	@Override
	public void close() throws HiveException {
	}

	@Override
	public void process(Object[] record) throws HiveException {

        List<Text> partial = (List<Text>) loi.getList(record[0]);

        for(int i=0;i<partial.size();i+=2){
            for(int j=0;j<partial.size();j+=2){
                if(i!=j) {
                    Text a1 = partial.get(i);
                    Text a2 = partial.get(i + 1);
                    Text b1 = partial.get(j);
                    Text b2 = partial.get(j + 1);
                    forward(new Object[]{a1,a2,b1,b2});
                }
            }
        }
	}
}
