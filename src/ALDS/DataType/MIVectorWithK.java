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
public class MIVectorWithK implements WritableComparable<MIVectorWithK>{

    private int k;
    private MIVector v;
    
    public MIVectorWithK(int k, MIVector v){
        super();
        this.k = k;
        this.v = v;
    }
    
    public MIVectorWithK(){
        super();
        this.v = new MIVector();
    }
    
    public int getK(){
        return this.k;
    }
    
    public MIVector getV(){
        return this.v;
    }
    @Override
    public void write(DataOutput d) throws IOException {
        d.write(this.k);
        v.write(d);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        this.k = di.readInt();
        v.readFields(di);
    }

    @Override
    public int compareTo(MIVectorWithK t) {
        if(this.k < t.getK())
            return -1;
        if (this.k > t.getK() )
            return 1;
        return 0;
    }
    
    @Override
    public String toString(){
        return "k: " + k + " v: " + v.toString();
    }
    
}
