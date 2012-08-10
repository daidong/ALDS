/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ALDS.DataType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author daidong
 */
public class EquipYPartitionPair implements WritableComparable<EquipYPartitionPair>{

    private EquipYPartitionKey eypk;
    private EquipYPartitionValue eypv;
    
    public EquipYPartitionPair(EquipYPartitionKey k, EquipYPartitionValue v){
        super();
        this.eypk = k;
        this.eypv = v;
    }
    
    public EquipYPartitionPair(){
        super();
        eypk = new EquipYPartitionKey();
        eypv = new EquipYPartitionValue();
    }
    
    @Override
    public void write(DataOutput d) throws IOException {
        eypk.write(d);
        eypv.write(d);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        eypk.readFields(di);
        eypv.readFields(di);
    }

    @Override
    public int compareTo(EquipYPartitionPair t) {
        return eypk.compareTo(t.eypk);
    }
    
    public EquipYPartitionKey getKey(){
        return this.eypk;
    }
    
    public EquipYPartitionValue getValue(){
        return this.eypv;
    }
}
