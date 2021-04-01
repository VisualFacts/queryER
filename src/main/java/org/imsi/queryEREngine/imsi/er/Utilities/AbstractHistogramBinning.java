package org.imsi.queryEREngine.imsi.er.Utilities;

import java.util.Arrays;
import java.util.Map;

public abstract class AbstractHistogramBinning {

	protected Map<String, Integer> tfIdf;
	protected int n;

	public AbstractHistogramBinning(Map<String, Integer> tfIdf) {
		this.tfIdf = tfIdf;
	}

	public void calculateNumberOfBins() {
		// Create frequency histogram
		Integer[] values = Arrays.stream(tfIdf.values().toArray())
				.map(Object::toString)
				.map(Integer::valueOf)
				.toArray(Integer[]::new);
		int firstOutlier = findFirstOutlier(values, 5);
		values = Arrays.stream(values).filter(val -> val > firstOutlier).toArray(Integer[]::new);
		int IQR = calculateIQR(values);
		int n = (int) ((2*IQR)/Math.cbrt(values.length));
		System.out.println("Number of bins " + n);
		this.n = n;
	}
	

	private int findFirstOutlier(Integer[] values, int threshold) {
		Integer val = values[0];
		int index = 1;
		int times = 1;
		while(true) {
			if(values[index] == val) {
				times ++;
				if(times == threshold) return values[index];
			}
			else times = 0;
			val = values[index];
			index ++;
		}
	}
	
	private static int percentile(Integer[] a, double percentile) {
		int index = (int) Math.ceil(percentile / 100.0 * a.length);
		return a[index-1];
	}

	private int calculateIQR(Integer[] a) {
		return percentile(a, 25) - percentile(a, 75);
	}
}
