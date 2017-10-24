package hortonworks.hdf.sam.custom.udf.datatype.conversion;

import com.hortonworks.streamline.streams.rule.UDF;

public class StringToDouble implements UDF<Double, String> {

	public Double evaluate(String value) {
		return Double.valueOf(value);
	}
}