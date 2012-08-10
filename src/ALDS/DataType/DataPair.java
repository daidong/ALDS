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

public class DataPair implements WritableComparable<DataPair> {
	private double x;
	private double y;

	public DataPair(double x, double y) {
		super();
		this.x = x;
		this.y = y;
	}

	public DataPair() {
		super();
	}

    public DataPair(DataPair old){
        super();
        this.x = old.getX();
        this.y = old.getY();
    }
    
    public String toString(){
        return "("+x+" ," + y+")";
    }
	public void write(DataOutput out) throws IOException {
		out.writeUTF(String.valueOf(x));
		out.writeUTF(String.valueOf(y));
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.x = Double.parseDouble(in.readUTF());
		this.y = Double.parseDouble(in.readUTF());
	}

	public int compareTo(DataPair o) {
		// TODO Auto-generated method stub
		DataPair that = (DataPair) o;
        if (this.y < that.y)
            return -1;
        if (this.y > that.y)
            return 1;
        if (this.x < that.x)
            return -1;
        if (this.x > that.x)
            return 1;
        return 0;
		
	}

	public double getX() {
		return this.x;
	}

	public void setX(double id){
        this.x = id;
    }

	public double getY(){
        return this.y;
    }
    
    public void setY(double id){
        this.y = id;
    }
}
