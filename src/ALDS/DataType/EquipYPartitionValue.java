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

public class EquipYPartitionValue implements WritableComparable<EquipYPartitionValue> {
	private double yStart;
	private double yEnd;
    private int number;
    

	public EquipYPartitionValue(double y1, double y2, int num) {
		super();
		this.yStart = y1;
		this.yEnd = y2;
        this.number = num;
	}

	public EquipYPartitionValue() {
		super();
		// TODO Auto-generated constructor stub
	}
    
    public EquipYPartitionValue(EquipYPartitionValue v){
        super();
        this.yStart = v.getYStart();
        this.yEnd = v.getYEnd();
        this.number = v.getNumber();
    }

    public String toString(){
        return "Start:"+yStart+"|End:"+yEnd+"|Num:"+number;
    }
    public void write(DataOutput out) throws IOException {
		out.writeUTF(String.valueOf(yStart));
		out.writeUTF(String.valueOf(yEnd));
        out.writeUTF(String.valueOf(number));
	}

	public void readFields(DataInput in) throws IOException {
		this.yStart = Double.parseDouble(in.readUTF());
		this.yEnd = Double.parseDouble(in.readUTF());
        this.number = Integer.parseInt(in.readUTF());
	}

    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;
        
        if (o instanceof EquipYPartitionValue){
            EquipYPartitionValue t = (EquipYPartitionValue) o;
            if (this.yStart == t.getYStart() && 
                    this.yEnd == t.getYEnd() &&
                    this.number == t.getNumber())
                return true;
        }
            
        return false;
    }
    

    @Override
    public int hashCode() {
        return this.number;
    }
    
	public int compareTo(EquipYPartitionValue o) {
		// TODO Auto-generated method stub
		EquipYPartitionValue that = (EquipYPartitionValue) o;
        if (this.yStart < that.yStart)
            return -1;
        if (this.yStart > that.yStart)
            return 1;
        if (this.yEnd < that.yEnd)
            return -1;
        if (this.yEnd > that.yEnd)
            return 1;
        return 0;
		
	}

	public double getYStart() {
		return yStart;
	}

	public void setYStart(double id){
        this.yStart = id;
    }

	public double getYEnd(){
        return this.yEnd;
    }
    
    public void setYEnd(double id){
        this.yEnd = id;
    }
    
    public int getNumber(){
        return this.number;
    }
    
    public void setNumber(int num){
        this.number = num;
    }
}
