package hortonworks.hdf.sam.custom.udf.datatype.conversion;

import com.hortonworks.streamline.streams.rule.UDF;

public class StringToLong implements UDF<Long, String> {

	public Long evaluate(String value) {
		return Long.valueOf(value);
	}
}