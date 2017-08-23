package hortonworks.hdf.sam.custom.udf.datatype.conversion;

import com.hortonworks.streamline.streams.rule.UDF;

public class IntToString implements UDF<String, Integer> {

	public String evaluate(Integer value) {
		return value.toString();
	}

}
