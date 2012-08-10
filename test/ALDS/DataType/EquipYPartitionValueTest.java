/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ALDS.DataType;

import java.util.HashMap;
import java.util.ArrayList;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author daidong
 */
public class EquipYPartitionValueTest {
    
    public EquipYPartitionValueTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }
    
    @Before
    public void setUp() {
    }

    /**
     * Test of toString method, of class EquipYPartitionValue.
     */
    @Test
    public void testToString() {
    }

    /**
     * Test of write method, of class EquipYPartitionValue.
     */
    @Test
    public void testWrite() throws Exception {
    }

    /**
     * Test of readFields method, of class EquipYPartitionValue.
     */
    @Test
    public void testReadFields() throws Exception {
    }

    /**
     * Test of equals method, of class EquipYPartitionValue.
     */
    @Test
    public void testEquals() {
        HashMap<EquipYPartitionValue, Integer> list = new HashMap<EquipYPartitionValue, Integer>();
        EquipYPartitionValue eypv = new EquipYPartitionValue(1,1,2);
        list.put(eypv, 1);
        EquipYPartitionValue eypv2 = new EquipYPartitionValue(2,1,2);
        list.put(eypv2, 2);
        
        EquipYPartitionValue eypv3 = new EquipYPartitionValue(1,1,2);
        assert(list.get(eypv3) != null);
    }

    /**
     * Test of compareTo method, of class EquipYPartitionValue.
     */
    @Test
    public void testCompareTo() {
    }

    /**
     * Test of getYStart method, of class EquipYPartitionValue.
     */
    @Test
    public void testGetYStart() {
    }

    /**
     * Test of setYStart method, of class EquipYPartitionValue.
     */
    @Test
    public void testSetYStart() {
    }

    /**
     * Test of getYEnd method, of class EquipYPartitionValue.
     */
    @Test
    public void testGetYEnd() {
    }

    /**
     * Test of setYEnd method, of class EquipYPartitionValue.
     */
    @Test
    public void testSetYEnd() {
    }

    /**
     * Test of getNumber method, of class EquipYPartitionValue.
     */
    @Test
    public void testGetNumber() {
    }

    /**
     * Test of setNumber method, of class EquipYPartitionValue.
     */
    @Test
    public void testSetNumber() {
    }
}
