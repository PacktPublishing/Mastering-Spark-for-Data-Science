/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.gzet.community.accumulo;

import org.apache.accumulo.core.client.mapreduce.InputFormatBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.format.DefaultFormatter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Map.Entry;

public class AccumuloGraphxInputFormat extends
		InputFormatBase<NullWritable, EdgeWritable> {
	@Override
	public RecordReader<NullWritable, EdgeWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		log.setLevel(getLogLevel(context));
		return new RecordReaderBase<NullWritable, EdgeWritable>() {
			@Override
			public boolean nextKeyValue() throws IOException,
					InterruptedException {
				if (scannerIterator.hasNext()) {
					++numKeysRead;

					Entry<Key, Value> entry = scannerIterator.next();
					EdgeWritable edge = new EdgeWritable();
					edge.setSourceVertex(entry.getKey().getRow().toString());
					edge.setDestVertex(entry.getKey().getColumnQualifier()
							.toString());
					edge.setCount(Long.parseLong(entry.getValue().toString()));

					currentK = NullWritable.get();
					currentKey = entry.getKey();
					currentV = edge;
					if (log.isTraceEnabled())
						log.trace("Processing key/value pair: "
								+ DefaultFormatter.formatEntry(entry, true));
					return true;
				}
				return false;
			}
		};
	}
}
