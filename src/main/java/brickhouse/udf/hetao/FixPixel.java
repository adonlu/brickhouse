package brickhouse.udf.hetao;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;



@Description(name = "fix_pixel",
		value = "_FUNC_(start,end,counts) - Returns start,count - counts for each pixel",

		extended = "return a pixel counts list")
public class FixPixel extends GenericUDTF {
	private PrimitiveObjectInspector startOI = null;
	private PrimitiveObjectInspector endOI = null;
	private PrimitiveObjectInspector countOI = null;

	@Override
	public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
		if (args.length != 3) {
			throw new UDFArgumentException("FixPixel() takes exactly three argument");
		}

		if (args[0].getCategory() != ObjectInspector.Category.PRIMITIVE
				&& ((PrimitiveObjectInspector) args[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.INT) {
			throw new UDFArgumentException("NameParserGenericUDTF() takes a string as a parameter");
		}
		if (args[1].getCategory() != ObjectInspector.Category.PRIMITIVE
				&& ((PrimitiveObjectInspector) args[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.INT) {
			throw new UDFArgumentException("NameParserGenericUDTF() takes a string as a parameter");
		}
		if (args[2].getCategory() != ObjectInspector.Category.PRIMITIVE
				&& ((PrimitiveObjectInspector) args[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.INT) {
			throw new UDFArgumentException("NameParserGenericUDTF() takes a string as a parameter");
		}

		// input
		startOI = (PrimitiveObjectInspector) args[0];
		endOI = (PrimitiveObjectInspector) args[0];
		countOI = (PrimitiveObjectInspector) args[0];

		// output
		List<String> fieldNames = new ArrayList<String>(3);
		List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(2);
		fieldNames.add("start");
		fieldNames.add("count");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
		return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
	}

	@Override
	public void process(Object[] record) throws HiveException {
		final int start =Integer.parseInt(startOI.getPrimitiveJavaObject(record[0]).toString());
		final int end = Integer.parseInt(endOI.getPrimitiveJavaObject(record[1]).toString());
		final int count =Integer.parseInt(countOI.getPrimitiveJavaObject(record[2]).toString());
		if(start>end){
			forward(null);
			return;
		}
		for(int i=start;i<end+1;i++){
			Object[] r=new Object[2];
			r[0]=i;
			r[1]=count;
			forward(r);
		}
	}

	@Override
	public void close() throws HiveException {
		// do nothing
	}
}