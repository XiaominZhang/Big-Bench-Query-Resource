package de.bankmark.bigbench.queries.q04;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableLongObjectInspector;
import org.apache.hadoop.io.LongWritable;
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
    private transient WritableConstantIntObjectInspector longOi = null;

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

		if (args.length != 2) {
			throw new UDFArgumentException(
					"sentiment() takes exactly 1 argument: {string} but was: "
							+ args.length + "\n" + args);
		}

		if (args[0].getCategory() != ObjectInspector.Category.LIST) {
			throw new UDFArgumentException(
					"sentiments() takes a long as first parameter");
		}

		loi = (StandardListObjectInspector) args[0];

        longOi = (WritableConstantIntObjectInspector)args[1];

        List<String> name = new ArrayList<String>();
        name.add("uid");
        name.add("item");
        name.add("wptype");
        name.add("tstamp");
        name.add("sessionid");
        List<ObjectInspector> oiList =new ArrayList<ObjectInspector>();
        oiList.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        oiList.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        oiList.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        oiList.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        oiList.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);

		StandardStructObjectInspector result = ObjectInspectorFactory
				.getStandardStructObjectInspector(name, oiList);
		return result;
	}

	@Override
	public void close() throws HiveException {
	}

	@Override
	public void process(Object[] record) throws HiveException {
        Integer timeout = longOi.get(record[1]);
		StandardStructObjectInspector soi = (StandardStructObjectInspector)loi.getListElementObjectInspector();
        List<Object> partial = (List<Object>) loi.getList(record[0]);
        int session=1;
        Long cur_time = null;
        for(int i = 0 ; i < partial.size(); i++){
            LongWritable a1 = (LongWritable)soi.getStructFieldData(partial.get(i), soi.getStructFieldRef("uid"));
            LongWritable a2 = (LongWritable)soi.getStructFieldData(partial.get(i), soi.getStructFieldRef("item"));
            Text a3 = (Text)soi.getStructFieldData(partial.get(i), soi.getStructFieldRef("wptype"));
            LongWritable a4 = (LongWritable)soi.getStructFieldData(partial.get(i), soi.getStructFieldRef("tstamp"));
            Text a5 = (Text)soi.getStructFieldData(partial.get(i), soi.getStructFieldRef("sessionid"));
            if(cur_time != null && a4.get() - cur_time > timeout){
                session++;
            }
            cur_time = a4.get();
            StringBuilder sb = new StringBuilder();
            sb.append(a5.toString());
            sb.append("_");
            sb.append(session);
            forward(new Object[]{a1,a2,a3,a4,new Text(sb.toString())});
        }
    }
}
