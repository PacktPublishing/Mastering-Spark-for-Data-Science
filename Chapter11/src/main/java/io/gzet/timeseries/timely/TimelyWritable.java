package io.gzet.timeseries.timely;

import com.google.common.collect.ComparisonChain;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TimelyWritable implements WritableComparable<TimelyWritable> {

	private Long time;
	private String metric;
	private String metricType;
	private double metricValue;

	public TimelyWritable() {
	}

	public TimelyWritable(Long time, String metric, String metricType, double metricValue) {
		this.time = time;
		this.metric = metric;
		this.metricType = metricType;
		this.metricValue = metricValue;
	}

	public Long getTime() {
		return time;
	}

	public void setTime(Long time) {
		this.time = time;
	}

	public String getMetric() {
		return metric;
	}

	public void setMetric(String metric) {
		this.metric = metric;
	}

	public String getMetricType() {
		return metricType;
	}

	public void setMetricType(String metricType) {
		this.metricType = metricType;
	}

	public double getMetricValue() {
		return metricValue;
	}

	public void setMetricValue(double metricValue) {
		this.metricValue = metricValue;
	}

	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeLong(time);
		dataOutput.write(metric.getBytes());
		dataOutput.write(metricType.getBytes());
		dataOutput.writeDouble(metricValue);
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		time = dataInput.readLong();
		metric = dataInput.readUTF();
		metricType = dataInput.readUTF();
		metricValue = dataInput.readDouble();
	}

	@Override
	public int compareTo(TimelyWritable that) {
		return ComparisonChain.start()
				.compare(this.time, that.time)
				.compare(this.metricValue, that.metricValue)
				.compare(this.metric, that.metric)
				.compare(this.metricType, that.metricType)
				.result();
	}

    @Override
    public String toString() {
        return "TimelyWritable{" +
                "time=" + time +
                ", metric='" + metric + '\'' +
                ", metricType='" + metricType + '\'' +
                ", metricValue=" + metricValue +
                '}';
    }
}
