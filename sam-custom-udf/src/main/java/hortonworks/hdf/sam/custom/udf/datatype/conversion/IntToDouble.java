package hortonworks.hdf.sam.custom.udf.datatype.conversion;

import com.hortonworks.streamline.streams.rule.UDF;

public class IntToDouble implements UDF<Double, Integer> {

	public Double evaluate(Integer value) {
		return value.doubleValue();
	}

}
