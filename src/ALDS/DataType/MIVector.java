/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
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
public class MIVector implements WritableComparable<MIVector>{

    private int size;
    private ArrayList<Double> vector;
    
    public ArrayList<Double> getVector(){
        return this.vector;
    }
    
    public MIVector(ArrayList<Double> v){
        super();
        this.size = v.size();
        this.vector = new ArrayList<Double>();
        for (Double vs : v)
            this.vector.add(vs);
    }
    
    public MIVector(MIVector old){
        super();
        this.size = old.getSize();
        this.vector = old.getVector();
    }
    public MIVector(){
        super();
        this.size = 0;
        this.vector = new ArrayList<Double>();
    }
    
    @Override
    public void write(DataOutput d) throws IOException {
        d.writeInt(this.size);
        for (int i = 0; i < this.size; i++){
            d.writeDouble(this.vector.get(i));
        }
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        this.size = di.readInt();
        this.vector = new ArrayList<Double>();
        for (int i = 0; i < this.size; i++){
            vector.add(di.readDouble());
        }
    }

    @Override
    public int compareTo(MIVector t) {
        if(this.size < t.getSize())
            return -1;
        if (this.size > t.getSize() )
            return 1;
        return 0;
    }
    
    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < this.size; i++){
            sb.append(String.valueOf(this.vector.get(i)) +"\n");
        }
        return sb.toString();
    }
    
    public int getSize(){
        return this.size;
    }
    
}
