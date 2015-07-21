package de.bankmark.bigbench.queries.q04;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.lazy.LazyLong;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;


public class transform1 extends AbstractGenericUDAFResolver {
    static final Log LOG = LogFactory.getLog(transform1.class);

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
//TODO: Type-checking

        return new GenericUDAFMovingAverageEvaluator();
    }


    public static class GenericUDAFMovingAverageEvaluator extends GenericUDAFEvaluator {
        //TODO: UDAF logic
// input inspectors for PARTIAL1 and COMPLETE
        private PrimitiveObjectInspector inputOI1;
        private PrimitiveObjectInspector inputOI2;
        private PrimitiveObjectInspector inputOI3;
        private PrimitiveObjectInspector inputOI4;

        // input inspectors for PARTIAL2 and FINAL
// list for MAs and one for residuals
        private StandardListObjectInspector loi;
        private StandardStructObjectInspector soi;
        private ObjectInspector uidOI;
        private ObjectInspector itemOI;
        private ObjectInspector wptypeOI;
        private ObjectInspector tstampOI;
        private ObjectInspector sessionidOI;
        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);
// initialize input inspectors
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE)
            {
                assert(parameters.length == 4);
                inputOI1 = (PrimitiveObjectInspector) parameters[0];
                inputOI2 = (PrimitiveObjectInspector) parameters[1];
                inputOI3 = (PrimitiveObjectInspector) parameters[2];
                inputOI4 = (PrimitiveObjectInspector) parameters[3];
            }
            else
            {
                loi = (StandardListObjectInspector) parameters[0];
                soi = (StandardStructObjectInspector)loi.getListElementObjectInspector();
                uidOI=soi.getStructFieldRef("uid").getFieldObjectInspector();
                itemOI=soi.getStructFieldRef("item").getFieldObjectInspector();
                wptypeOI=soi.getStructFieldRef("wptype").getFieldObjectInspector();
                tstampOI=soi.getStructFieldRef("tstamp").getFieldObjectInspector();
                sessionidOI=soi.getStructFieldRef("sessionid").getFieldObjectInspector();
            }
// init output object inspectors
            //if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
// The output of a partial aggregation is a list of doubles representing the
// moving average being constructed.
// the first element in the list will be the window size
//
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

            return ObjectInspectorFactory.getStandardListObjectInspector(
                    ObjectInspectorFactory.getStandardStructObjectInspector(name, oiList));
            //}
            //else {
// The output of FINAL and COMPLETE is a full aggregation, which is a
            //    return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
            //}
        }
        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            MaAgg myagg = (MaAgg) agg;
            myagg.sortItems();
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
                List<Object> partialMovingAverage = (List<Object>) loi.getList(partial);
                myagg.merge(partialMovingAverage);
            }
        }
        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            MaAgg myagg = (MaAgg) agg;
            Long a1 = PrimitiveObjectInspectorUtils.getLong(parameters[0], inputOI1);
            Long a2 = PrimitiveObjectInspectorUtils.getLong(parameters[1], inputOI2);
            String a3 = PrimitiveObjectInspectorUtils.getString(parameters[2], inputOI3);
            Long a4 = PrimitiveObjectInspectorUtils.getLong(parameters[3], inputOI4);
            myagg.items.add(new Item(a1,a2,a3,a4));
        }

        class Item implements Comparable<Item>{

            Long uid;
            Long itemId;
            String wpType;
            Long tstamp;
            String sessionId;
            Item(Long uid, Long itemId, String wpType,Long tstamp, String sessionid){
                this.uid=uid;
                this.itemId=itemId;
                this.wpType=wpType;
                this.tstamp=tstamp;
                this.sessionId=sessionid;
            }

            Item(Long uid, Long itemId, String wpType,Long tstamp){
                this(uid,itemId,wpType,tstamp,Long.toString(uid));
            }

            @Override
            public int compareTo(Item o) {
                return this.tstamp.compareTo(o.tstamp);
            }

            Object[] serialize(){
                Object[] a = new Object[5];
                a[0]=new LongWritable(uid);
                a[1]=new LongWritable(itemId);
                a[2]=new Text(wpType);
                a[3]=new LongWritable(tstamp);
                a[4]=new Text(sessionId);
                return a;
            }
        }


        // Aggregation buffer definition and manipulation methods
        class MaAgg implements AggregationBuffer {
            ArrayList<Item> items;

            ArrayList<Object[]> serialize(){
                ArrayList<Object[]> results = new ArrayList<Object[]>();
                for (Item i : items) {
                    results.add(i.serialize());
                }
                return results;
            }

            void sortItems(){
                Collections.sort(items);
            }

            void merge(List<Object> partial){
                int orgSize = items.size();
                ArrayList<Item> toMerge = new ArrayList<Item>();
                for(int i = 0 ; i < partial.size(); i++){
                    LongWritable a1 = (LongWritable)soi.getStructFieldData(partial.get(i), soi.getStructFieldRef("uid"));
                    LongWritable a2 = (LongWritable)soi.getStructFieldData(partial.get(i), soi.getStructFieldRef("item"));
                    Text a3 = (Text)soi.getStructFieldData(partial.get(i), soi.getStructFieldRef("wptype"));
                    LongWritable a4 = (LongWritable)soi.getStructFieldData(partial.get(i), soi.getStructFieldRef("tstamp"));
                    Text a5 = (Text)soi.getStructFieldData(partial.get(i), soi.getStructFieldRef("sessionid"));
                    toMerge.add(new Item(a1.get(),a2.get(),a3.toString(),a4.get(),a5.toString()));
                    items.add(null);
                }

                int k = toMerge.size() + orgSize - 1;
                int i = orgSize - 1; // Index of last element in array b
                int j = toMerge.size() - 1; // Index of last element in array a
// Start comparing from the last element and merge a and b
                while (i >= 0 && j >= 0) {
                    if (items.get(i).compareTo(toMerge.get(j)) > 0) {
                        items.set(k--, items.get(i--));
                    } else {
                        items.set(k--, toMerge.get(j--));
                    }
                }
                while (j >= 0) {
                    items.set(k--, toMerge.get(j--));
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
            myagg.items = new ArrayList<Item>();
            myagg.items.clear();
        }
    }
}
