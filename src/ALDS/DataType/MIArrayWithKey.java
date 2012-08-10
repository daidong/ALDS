/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ALDS.DataType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author daidong
 */
public class MIArrayWithKey implements WritableComparable<MIArrayWithKey>{

    //private int mapperId;
    //private int parId;
    private int key;
    private MIArray array = null;
    
    public MIArrayWithKey(){
        super();
        //this.mapperId = -1;
        //this.parId = -1;
        this.key = -1;
        this.array = new MIArray();
    }
    
    public MIArrayWithKey(int key, MIArray array){
        super();
        //this.mapperId = mid;
        //this.parId = pid;
        this.key = key;
        this.array = array;
    }
    
    @Override
    public void write(DataOutput d) throws IOException {
        d.writeInt(key);
        //d.writeInt(mapperId);
        //d.writeInt(parId);
        array.write(d);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        //mapperId = di.readInt();
        //parId = di.readInt();
        key = di.readInt();
        array.readFields(di);
    }

    @Override
    public int compareTo(MIArrayWithKey t) {
        if (this.key < t.getKey())
            return -1;
        if (this.key > t.getKey())
            return 1;
        return 0;
    }
    
    public int getKey(){
        return this.key;
    }
    /*
    public int getMapperId(){
        return this.mapperId;
    }
    
    public int getParId(){
        return this.parId;
    }
    */
    public MIArray getArray(){
        return this.array;
    }
}
