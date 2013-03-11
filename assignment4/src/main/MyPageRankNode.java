/*
 * Cloud9: A Hadoop toolkit for working with big data
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;

/**
 * Representation of a graph node for PageRank. 
 *
 * @author Jimmy Lin
 * @author Michael Schatz
 */

/* added the array for multiple sources */

public class MyPageRankNode implements Writable {
  public static enum Type {
    Complete((byte) 0),  // PageRank mass and adjacency list.
    Mass((byte) 1),      // PageRank mass only.
    Structure((byte) 2); // Adjacency list only.

    public byte val;

    private Type(byte v) {
      this.val = v;
    }
  };

	private static final Type[] mapping = new Type[] { Type.Complete, Type.Mass, Type.Structure };

	private Type type;
	private int nodeid;
	private int ns; // specify number of sources
	private float[] pagerank;
	private ArrayListOfIntsWritable adjacenyList;
	private int sourceid; // indicate which pagerank value is being processed

	public MyPageRankNode() {}

  /* 
	public MyPageRankNode(int ns) {
		pagerank = new float[ns];
	}
	*/
	
	public float[] getPageRank() {
		return pagerank;
	}

	public void setPageRank(float[] p) {
		this.pagerank = p;
		ns = p.length;
	}

	public int getNodeId() {
		return nodeid;
	}

	public void setNodeId(int n) {
		this.nodeid = n;
	}
	
	public int getSourceId() {
		return sourceid;
	}
	
	public void setSourceId(int k) {
		sourceid = k;
	}

	public ArrayListOfIntsWritable getAdjacenyList() {
		return adjacenyList;
	}

	public void setAdjacencyList(ArrayListOfIntsWritable list) {
		this.adjacenyList = list;
	}

	public Type getType() {
		return type;
	}

	public void setType(Type type) {
		this.type = type;
	}

	/**
	 * Deserializes this object.
	 *
	 * @param in source for raw byte representation
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		int b = in.readByte();
		type = mapping[b];
		nodeid = in.readInt();
		ns = in.readInt();

		pagerank = new float[ns];
		if (type.equals(Type.Mass)) {
			for (int k = 0; k < ns; k++) {
				pagerank[k] = in.readFloat();
			}
			return;
		}

		if (type.equals(Type.Complete)) {
			for (int k = 0; k < ns; k++) {
				pagerank[k] = in.readFloat();
			}
		}

		adjacenyList = new ArrayListOfIntsWritable();
		adjacenyList.readFields(in);
	}

	/**
	 * Serializes this object.
	 *
	 * @param out where to write the raw byte representation
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeByte(type.val);
		out.writeInt(nodeid);
		out.writeInt(ns);

		if (type.equals(Type.Mass)) {
			for (int k = 0; k < ns; k++) {
				out.writeFloat(pagerank[k]);
			}
			return;
		}

		if (type.equals(Type.Complete)) {
			for (int k = 0; k < ns; k++) {
				out.writeFloat(pagerank[k]);
			}
		}

		adjacenyList.write(out);
	}

	@Override
	public String toString() {
		String str = new String();
		for (int k = 0; k < ns; k++) {
			str += String.format("{%d %.4f %s}\n",
					nodeid, pagerank[k], (adjacenyList == null ? "[]" : adjacenyList.toString(10)));
		}
		return str;
	}


  /**
   * Returns the serialized representation of this object as a byte array.
   *
   * @return byte array representing the serialized representation of this object
   * @throws IOException
   */
  public byte[] serialize() throws IOException {
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(bytesOut);
    write(dataOut);

    return bytesOut.toByteArray();
  }

  /**
   * Creates object from a <code>DataInput</code>.
   *
   * @param in source for reading the serialized representation
   * @return newly-created object
   * @throws IOException
   */
  public static MyPageRankNode create(DataInput in) throws IOException {
    MyPageRankNode m = new MyPageRankNode();
    m.readFields(in);

    return m;
  }

  /**
   * Creates object from a byte array.
   *
   * @param bytes raw serialized representation
   * @return newly-created object
   * @throws IOException
   */
  public static MyPageRankNode create(byte[] bytes) throws IOException {
    return create(new DataInputStream(new ByteArrayInputStream(bytes)));
  }
}
