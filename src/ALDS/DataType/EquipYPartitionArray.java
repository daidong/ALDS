
package ALDS.DataType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.ArrayList;
import org.apache.hadoop.io.WritableComparable;
/**
 *
 * @author daidong
 */

public class EquipYPartitionArray implements WritableComparable<EquipYPartitionArray> {
	private ArrayList<DataPair> pa;
    

	public EquipYPartitionArray() {
		super();
		this.pa = new ArrayList<DataPair>();
	}


	public void write(DataOutput out) throws IOException {
        for (DataPair p : pa)
            p.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		in.readByte();
	}

	public int compareTo(EquipYPartitionArray o) {
		
        return 0;
		
	}


}
