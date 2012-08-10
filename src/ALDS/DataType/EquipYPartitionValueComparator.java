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
public class EquipYPartitionValueComparator implements Comparator<EquipYPartitionValue> {

    @Override
    public int compare(EquipYPartitionValue t, EquipYPartitionValue t1) {
        EquipYPartitionValue first = (EquipYPartitionValue) t;
        EquipYPartitionValue second = (EquipYPartitionValue) t1;
        if (first.getNumber() < second.getNumber()) {
            return -1;
        }
        if (first.getNumber() > second.getNumber()) {
            return 1;
        }
        return 0;
    }

}