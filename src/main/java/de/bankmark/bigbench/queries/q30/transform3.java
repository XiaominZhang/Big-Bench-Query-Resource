package de.bankmark.bigbench.queries.q30;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class transform3 extends GenericUDTF {

    static final Log LOG = LogFactory.getLog(transform3.class);

	// objects created in initialize()
    private transient StandardListObjectInspector loi = null;

	public transform3() {
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
        fieldNames.add("affine_category_id");
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

        for(int i=0;i<partial.size();i++){
            for(int j=0;j<partial.size();j++){
                if(i!=j) {
                    Text a1 = partial.get(i);
                    Text b1 = partial.get(j);
                    forward(new Object[]{a1,b1});
                }
            }
        }
	}
}
