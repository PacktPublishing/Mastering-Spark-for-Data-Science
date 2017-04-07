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

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class EdgeWritable implements WritableComparable<EdgeWritable> {

	private String sourceVertex;
	private String destVertex;
	private Long count;

	public EdgeWritable() {
	}

	public EdgeWritable(String sourceVertex, String destVertex, Long count) {
		this.sourceVertex = sourceVertex;
		this.destVertex = destVertex;
		this.count = count;
	}

	public void readFields(DataInput dataInput) throws IOException {
		sourceVertex = WritableUtils.readString(dataInput);
		destVertex = WritableUtils.readString(dataInput);
		count = WritableUtils.readVLong(dataInput);
	}

	public void write(DataOutput dataOutput) throws IOException {
		WritableUtils.writeString(dataOutput, sourceVertex);
		WritableUtils.writeString(dataOutput, destVertex);
		WritableUtils.writeVLong(dataOutput, count);
	}

	@Override
	public int compareTo(EdgeWritable edgeWritable) {

		int result = sourceVertex.compareTo(edgeWritable.sourceVertex);
		if (0 == result) {
			result = destVertex.compareTo(edgeWritable.destVertex);
		}
		if (0 == result) {
			result = Long.compare(count, edgeWritable.count);
		}
		return result;
	}

	public String getSourceVertex() {
		return sourceVertex;
	}

	public void setSourceVertex(String sourceVertex) {
		this.sourceVertex = sourceVertex;
	}

	public String getDestVertex() {
		return destVertex;
	}

	public void setDestVertex(String destVertex) {
		this.destVertex = destVertex;
	}
	
	public Long getCount(){
		return count;
	}

	public void setCount(Long count){
		this.count = count;
	}
	
	@Override
	public String toString() {

		return (new StringBuilder().append(sourceVertex).append(",")
				.append(destVertex)).append(",").append(count).toString();
	}
}
