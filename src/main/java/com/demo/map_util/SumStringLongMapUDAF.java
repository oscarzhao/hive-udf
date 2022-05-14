package com.demo.map_util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import java.util.HashMap;


@Description(
        name     = "sum_string_long_map",
        value    = "_FUNC_( value) : Sum the long value by key in the map.",
        extended = "Example:\n" +
                "    SELECT _FUNC_(metric_data) as metric_data FROM events;\n" +
                "    (returns {\"a\":2, \"b\":1} if the metric_data was [{\"a\":1, \"b\":1}, {\"a\":1}])\n\n"
)
public class SumStringLongMapUDAF extends AbstractGenericUDAFResolver {
    static final Log LOG = LogFactory.getLog(SumStringLongMapUDAF.class.getName());

    // doc: https://blog.dataiku.com/2013/05/01/a-complete-guide-to-writing-hive-udf#:~:text=Generic%20Hive%20UDF,by%20extending%20the%20GenericUDF%20class.&text=A%20key%20concept%20when%20working,around%20using%20the%20Object%20type.

    @Override
    public SumStringLongMapEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        // type check goes here
        if (parameters.length != 1) {
            throw new UDFArgumentTypeException(parameters.length - 1, "Exactly one argument is expected.");
        }

        ObjectInspector oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(parameters[0]);
        if (oi.getCategory() != ObjectInspector.Category.MAP) {
            throw new UDFArgumentTypeException(0,
                    "Argument must be MAP, but "
                            + oi.getCategory().name()
                            + " was passed.");
        }

        MapObjectInspector inputOI = (MapObjectInspector) oi;
        ObjectInspector keyOI = inputOI.getMapKeyObjectInspector();
        ObjectInspector valueOI = inputOI.getMapValueObjectInspector();

        if (keyOI.getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0,
                    "Map key must be PRIMITIVE, but "
                            + keyOI.getCategory().name()
                            + " was passed.");
        }
        PrimitiveObjectInspector inputKeyOI = (PrimitiveObjectInspector) keyOI;
        if (inputKeyOI.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentTypeException(0,
                    "Map value must be STRING, but "
                            + inputKeyOI.getPrimitiveCategory().name()
                            + " was passed.");
        }

        if (valueOI.getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0,
                    "Map value must be PRIMITIVE, but "
                            + valueOI.getCategory().name()
                            + " was passed.");
        }
        PrimitiveObjectInspector inputValueOI = (PrimitiveObjectInspector) valueOI;

        if (inputValueOI.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.LONG) {
            throw new UDFArgumentTypeException(0,
                    "Map value must be LONG (BIGINT), but "
                            + inputValueOI.getPrimitiveCategory().name()
                            + " was passed.");
        }

        return new SumStringLongMapEvaluator();
    }

    static class SumMapAgg extends GenericUDAFEvaluator.AbstractAggregationBuffer {
        HashMap<String, Long> resultMap;
    }

    public static class SumStringLongMapEvaluator extends GenericUDAFEvaluator {
        MapObjectInspector inputOI;
        ObjectInspector keyOI;
        ObjectInspector valueOI;

        MapObjectInspector outputOI;

        // UDAF logic goes here
        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            assert (parameters.length == 1);
            super.init(m, parameters);

            inputOI = (MapObjectInspector) parameters[0];
            keyOI = inputOI.getMapKeyObjectInspector();
            valueOI = inputOI.getMapValueObjectInspector();
            outputOI = (MapObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(inputOI, ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA);

            LOG.info("inputOI=" + inputOI.getClass().toString() + "<" + keyOI.toString() + "," + valueOI.toString() + ">");
            LOG.info("outputOI=" + outputOI.toString());
            return outputOI;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            SumMapAgg newAgg = new SumMapAgg();
            reset(newAgg);
            return newAgg;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            SumMapAgg myagg = (SumMapAgg) agg;
            myagg.resultMap = new HashMap<>(8);
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            if (parameters == null) {
                return;
            }
            assert (parameters.length == 1);

            SumMapAgg myagg = (SumMapAgg) agg;
            if (myagg == null) {
                return;
            }

            LOG.info("parameter="+ parameters[0].toString() + ", class=" + parameters[0].getClass());
            HashMap<String, Long> partialMap = (HashMap<String, Long>)inputOI.getMap(parameters[0]);
            partialMap.forEach((key,val)-> {
                LOG.info("iterate partialMap, key=" + key + ", value=" + val.toString());
                Long baseValue = myagg.resultMap.getOrDefault(key, 0L);
                Long partialValue = partialMap.getOrDefault(key, 0L);
                myagg.resultMap.put(key, baseValue + partialValue);
            });
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            return terminate(agg);
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial == null) {
                return;
            }
            SumMapAgg myagg = (SumMapAgg) agg;
            if (myagg == null) {
                return;
            }

            HashMap<String, Long> partialMap = (HashMap<String, Long>)inputOI.getMap(partial);
            partialMap.forEach((key,val)-> {
                LOG.info("merge partialMap, key=" + key + ", value=" + val.toString());
                Long baseValue = myagg.resultMap.getOrDefault(key, 0L);
                Long partialValue = partialMap.getOrDefault(key, 0L);
                myagg.resultMap.put(key, baseValue + partialValue);
            });
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            SumMapAgg myagg = (SumMapAgg) agg;
            if (myagg == null) {
                return null;
            }
            return myagg.resultMap;
        }
    }
}