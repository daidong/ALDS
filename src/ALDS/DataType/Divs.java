package ALDS.DataType;

import java.io.DataInput;


public class Divs {
	public int Colacc[];
	public int Col[];
	public int box[][];
	
	public Divs(int size)
	{
		Colacc = new int[size];
		Col = new int[size];
		box = new int[size][size];
	}
}