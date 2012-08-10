/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ALDS.DataType;

import java.util.Comparator;

/**
 *
 * @author daidong
 */
public class DataPairYComparator implements Comparator<DataPair>{

    @Override
    public int compare(DataPair t, DataPair t1) {
        
        if (t.getY() < t1.getY()) {
            return -1;
        }
        if (t.getY() > t1.getY()) {
            return 1;
        }
        return 0;
        
    }
    
}


