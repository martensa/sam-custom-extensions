package hortonworks.hdf.sam.custom.udf.math;

import com.hortonworks.streamline.streams.rule.UDF2;

public class Add implements UDF2<Double, Double, Double> {

	public Double evaluate(Double value1, Double value2) {
		return value1 + value2;
	}
}