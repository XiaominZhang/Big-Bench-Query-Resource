package de.bankmark.bigbench.queries.q30;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class transform1 extends AbstractGenericUDAFResolver {
    static final Log LOG = LogFactory.getLog(transform1.class);

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
//TODO: Type-checking
//We need exactly three parameters
        if (parameters.length != 2) {
            throw new UDFArgumentTypeException(parameters.length - 1, "Moving Average requires 2 parameters");
        }
//check the first parameter to make sure they type is numeric
        if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE)
        {
            throw new UDFArgumentTypeException(0, "Only primitive, numeric types can have a moving average but "+
                    parameters[0].getTypeName() + "was passed.");
        }
// if it's a primative, let's make sure it's numeric
        switch(((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory()) {
//fall through all numeric primitives
            case INT:
                break;
            default:
                throw new UDFArgumentTypeException(0, "Only numeric type arguments (excluding bytes and timestamps) are accepted"+
                        "but " + parameters[0].getTypeName() + " was passed.");
        }
// check the second parameter
        if (parameters[1].getCategory() != ObjectInspector.Category.PRIMITIVE)
        {
            throw new UDFArgumentTypeException(0, "Only primitive, numeric types can have a moving average but "+
                    parameters[1].getTypeName() + "was passed.");
        }
// if it's a primative, let's make sure it's numeric
        switch(((PrimitiveTypeInfo) parameters[1]).getPrimitiveCategory()) {
//fall through all numeric primitives
            case STRING:
                break;
            default:
                throw new UDFArgumentTypeException(0, "Only numeric type arguments (excluding bytes and timestamps) are accepted"+
                        "but " + parameters[1].getTypeName() + " was passed.");
        }
        return new GenericUDAFMovingAverageEvaluator();
    }


    public static class GenericUDAFMovingAverageEvaluator extends GenericUDAFEvaluator {
        //TODO: UDAF logic
// input inspectors for PARTIAL1 and COMPLETE
        private PrimitiveObjectInspector inputOI1;
        private PrimitiveObjectInspector inputOI2;
        // input inspectors for PARTIAL2 and FINAL
// list for MAs and one for residuals
        private StandardListObjectInspector loi;
        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);
// initialize input inspectors
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE)
            {
                assert(parameters.length == 2);
                inputOI1 = (PrimitiveObjectInspector) parameters[0];
                inputOI2 = (PrimitiveObjectInspector) parameters[1];
            }
            else
            {
                loi = (StandardListObjectInspector) parameters[0];
            }

            return ObjectInspectorFactory.getStandardListObjectInspector(
                        PrimitiveObjectInspectorFactory.writableStringObjectInspector);

        }
        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            MaAgg myagg = (MaAgg) agg;
            return myagg.serialize();
        }
        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
// final return value goes here
            MaAgg myagg = (MaAgg) agg;
            return myagg.serialize();
        }
        @SuppressWarnings("unchecked")
        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
// if we're merging two separate sets we're creating one table that's doubly long
            if (partial != null)
            {
                MaAgg myagg = (MaAgg) agg;
                List<Text> partialMovingAverage = (List<Text>) loi.getList(partial);
                myagg.merge(partialMovingAverage);
            }
        }
        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            MaAgg myagg = (MaAgg) agg;
            if(parameters[0] != null) {
                Integer p = PrimitiveObjectInspectorUtils.getInt(parameters[0], inputOI1);
                if (!myagg.items.containsKey(p)) {
                    if(parameters[1] != null) {
                        String v = PrimitiveObjectInspectorUtils.getString(parameters[1], inputOI2);
                        myagg.items.put(p,v);
                    }
                }
            }

        }

        // Aggregation buffer definition and manipulation methods
        static class MaAgg implements AggregationBuffer {
            HashMap<Integer, String> items;

            ArrayList<Text> serialize(){
                ArrayList<Text> results = new ArrayList<Text>();
                for (Integer k : items.keySet()) {
                    results.add(new Text(k.toString()));
                    results.add(new Text(items.get(k)));
                }
                return results;
            }

            void merge(List<Text> partial){
                if(partial.size() < 1){
                    return;
                }
                if(items.size() < 1){
                    for(int i = 0 ; i < partial.size(); i+=2){
                        items.put(Integer.parseInt(partial.get(i).toString()), partial.get(i + 1).toString());
                    }
                }
                else {
                    for(int i = 0 ; i < partial.size(); i+=2){
                        String k = partial.get(i).toString();
                        if(!items.containsKey(Integer.parseInt(k))) {
                            items.put(Integer.parseInt(k), partial.get(i+1).toString());
                        }
                    }
                }
            }
        };
        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            MaAgg result = new MaAgg();
            reset(result);
            return result;
        }
        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            MaAgg myagg = (MaAgg) agg;
            myagg.items = new HashMap<Integer, String>();
            myagg.items.clear();
        }
    }
}
