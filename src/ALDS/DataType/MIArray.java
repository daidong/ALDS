/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ALDS.DataType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author daidong
 */
public class MIArray implements WritableComparable<MIArray>{
    
    private int size;
    private double[] array;
    //private int[] par;
    
    public MIArray(){
        super();
        size = 0;
        array = null;
        //par = null;
    }
    /*
    public MIArray(int[] part){
        super();
        size = part.length;
        par = part;
    }
    */
    
    public MIArray(double [] values){
        super();
        size = values.length;
        array = values;
    }
    
    public double[] get(){
        return this.array;
    }
    
    public int getSize(){
        return this.size;
    }
    
    public String toString(){
        String rtn = "";
        for (int i = 0; i < array.length; i++){
            if (array[i] > 0)
                rtn += (array[i] + "-");
        }
        rtn += "\n";
        return rtn;
    }

    @Override
    public void write(DataOutput d) throws IOException {
        d.writeInt(size);
        for (int i = 0; i < size; i++){
            d.writeDouble(array[i]);
            //d.writeInt(par[i]);
        }
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        size = di.readInt();
        array = new double[size];
        //par = new int[size];
        for (int i = 0; i < size; i++){
            array[i] = di.readDouble();
            //par[i] = di.readInt();
        }
    }

    @Override
    public int compareTo(MIArray t) {
        if(this.size < t.getSize())
            return -1;
        if (this.size > t.getSize() )
            return 1;
        return 0;
    }
}
