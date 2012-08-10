package ALDS.DataType;

import java.io.DataInput;

public class Partition {
	public Double Q[];
	public Double P[];
	public int Col[];
	public int Colacc[];
	public int box[][];
	
	public Partition(int size)
	{
		Q =  new Double[size];
		
        P = new Double[size];
		Col = new int[size];
		Colacc = new int[size];
		box =  new int[size][size];
	}

}
