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
public class DataPairXComparator implements Comparator<DataPair>{

    @Override
    public int compare(DataPair t, DataPair t1) {
        
        if (t.getX() < t1.getX()) {
            return -1;
        }
        if (t.getX() > t1.getX()) {
            return 1;
        }
        return 0;
        
    }
    
}
