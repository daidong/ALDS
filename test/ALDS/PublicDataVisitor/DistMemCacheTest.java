/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ALDS.PublicDataVisitor;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author daidong
 */
public class DistMemCacheTest {
    
    public DistMemCacheTest() {
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
     * Test of get method, of class DistMemCache.
     */
    @Test
    public void testGet() {
        DistMemCache test = new DistMemCache();
        for (int i = 0; i < 100; i++){
            System.out.println("Get " + i + "th: " + test.get(String.valueOf(i)));
        }
    }

    /**
     * Test of set method, of class DistMemCache.
     */
    @Test
    public void testSet() {
    }

    /**
     * Test of exist method, of class DistMemCache.
     */
    @Test
    public void testExist() {
    }
}
