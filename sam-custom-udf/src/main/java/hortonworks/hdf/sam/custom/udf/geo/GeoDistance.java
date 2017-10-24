package hortonworks.hdf.sam.custom.udf.geo;

import com.hortonworks.streamline.streams.rule.UDF4;

public class GeoDistance implements UDF4<Double, Double, Double, Double, Double> {

	public Double evaluate(Double lat1, Double lng1, Double lat2, Double lng2) {
		double x1 = Math.toRadians(lat1);
		double y1 = Math.toRadians(lng1);
		double x2 = Math.toRadians(lat2);
		double y2 = Math.toRadians(lng2);

		double dlon = y2 - y1;
		double dlat = x2 - x1;

		double a = Math.pow((Math.sin(dlat / 2)), 2)
				+ Math.cos(lat1) * Math.cos(lat2) * Math.pow(Math.sin(dlon / 2), 2);
		double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

		return 3958.75 * c;
	}
}