package hortonworks.hdf.sam.custom.udf.math;

import com.hortonworks.streamline.streams.rule.UDF;

public class Absolute implements UDF<Double, Double> {

	public Double evaluate(Double value) {
		return Math.abs(value);
	}
}