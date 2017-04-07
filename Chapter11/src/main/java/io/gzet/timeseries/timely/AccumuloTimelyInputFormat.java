package io.gzet.timeseries.timely;

import org.apache.accumulo.core.client.lexicoder.LongLexicoder;
import org.apache.accumulo.core.client.lexicoder.PairLexicoder;
import org.apache.accumulo.core.client.lexicoder.StringLexicoder;
import org.apache.accumulo.core.client.mapreduce.InputFormatBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.ComparablePair;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map.Entry;

public class AccumuloTimelyInputFormat extends
		InputFormatBase<NullWritable, TimelyWritable> {
	@Override
	public RecordReader<NullWritable, TimelyWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {

		return new RecordReaderBase<NullWritable, TimelyWritable>() {

            @Override
			public boolean nextKeyValue() throws IOException, InterruptedException {
				if (scannerIterator.hasNext()) {
					++numKeysRead;

					Entry<Key, Value> entry = scannerIterator.next();
                    TimelyWritable timely = new TimelyWritable();

                    PairLexicoder<String, Long> rowCoder = new PairLexicoder<>(new StringLexicoder(), new LongLexicoder());
                    ComparablePair<String, Long> row = rowCoder.decode(entry.getKey().getRow().getBytes());

                    timely.setTime(row.getSecond());
                    timely.setMetricValue(ByteBuffer.wrap(entry.getValue().get()).getDouble());
                    timely.setMetricType(entry.getKey().getColumnFamily().toString());
                    timely.setMetric(row.getFirst());

                    currentK = NullWritable.get();
					currentKey = entry.getKey();
					currentV = timely;
					return true;
				}
				return false;
			}
		};
	}
}
