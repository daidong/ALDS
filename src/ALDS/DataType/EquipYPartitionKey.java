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

public class EquipYPartitionKey implements WritableComparable<EquipYPartitionKey> {
    private int ys;
    private int mapId;
	private int dividId;

	public EquipYPartitionKey(int ys, int mapId, int dividId) {
		super();
        this.ys = ys;
		this.mapId = mapId;
		this.dividId = dividId;
	}

	public EquipYPartitionKey() {
		super();
	}
    
    public EquipYPartitionKey(EquipYPartitionKey k){
        super();
        this.ys = k.getYs();
        this.mapId = k.getMapId();
        this.dividId = k.getDividId();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;
        
        if (o instanceof EquipYPartitionKey){
            EquipYPartitionKey t = (EquipYPartitionKey) o;
            if (this.ys == t.getYs() && 
                    this.mapId == t.getMapId() &&
                    this.dividId == t.getDividId())
                return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return ys + mapId + dividId;
    }
    
    public String toString(){
        return "ys"+ys+"|mapId:"+mapId+"|dividId:"+dividId;
    }
	public void write(DataOutput out) throws IOException {
        out.writeUTF(String.valueOf(ys));
		out.writeUTF(String.valueOf(mapId));
		out.writeUTF(String.valueOf(dividId));
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
        this.ys = Integer.parseInt(in.readUTF());
		this.mapId = Integer.parseInt(in.readUTF());
		this.dividId = Integer.parseInt(in.readUTF());
	}

	public int compareTo(EquipYPartitionKey o) {
		// TODO Auto-generated method stub
		EquipYPartitionKey that = (EquipYPartitionKey) o;
        if (this.ys < that.ys)
            return -1;
        if (this.ys > that.ys)
            return 1;
        if (this.mapId < that.mapId)
            return -1;
        if (this.mapId > that.mapId)
            return 1;
        if (this.dividId < that.dividId)
            return -1;
        if (this.dividId > that.dividId)
            return 1;
        return 0;
		
	}

	public int getMapId() {
		return mapId;
	}

	public void setMapId(int id){
        this.mapId = id;
    }

	public int getDividId(){
        return this.dividId;
    }
    
    public void setDividId(int id){
        this.dividId = id;
    }
    
    public int getYs(){
        return this.ys;
    }
    
    public void setYs(int ys){
        this.ys = ys;
    }
}
