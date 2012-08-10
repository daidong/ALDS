/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ALDS;

import ALDS.DataType.DataPair;
import ALDS.DataType.EquipYPartitionKey;
import ALDS.DataType.EquipYPartitionPair;
import ALDS.DataType.EquipYPartitionValue;
import ALDS.DataType.EquipYPartitionValueComparator;
import ALDS.PublicDataVisitor.DistMemCache;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
/**
 *
 * @author daidong
 */
public class EquiPartitionYAxisJoin {
    
    public static class MapClassY2 extends Mapper<EquipYPartitionKey, EquipYPartitionValue, IntWritable, EquipYPartitionPair>{
        
        @Override
        public void map(EquipYPartitionKey k, EquipYPartitionValue v, Context context) throws IOException, InterruptedException{
            
            System.out.println("key: " + k + " value: " + v);
            context.write(new IntWritable(k.getYs()), new EquipYPartitionPair(k, v));
            
        }
    }
    
    public static class ReduceClassY2 extends Reducer<IntWritable, EquipYPartitionPair, IntWritable, EquipYPartitionValue>{
        
        public DistMemCache writeResults = new DistMemCache();
        
        @Override
        public void reduce(IntWritable k, Iterable<EquipYPartitionPair> values, Context context) throws IOException, InterruptedException{
            
            Configuration conf = context.getConfiguration();
            int dividerY = k.get();
            int nodeNumber = conf.getInt("nodes", 100);
            
            HashMap<EquipYPartitionKey, EquipYPartitionValue> beforeOptimal = new HashMap<EquipYPartitionKey,EquipYPartitionValue>();
            HashMap<EquipYPartitionValue, EquipYPartitionKey> revertLookup = new HashMap<EquipYPartitionValue, EquipYPartitionKey>();
            
            List<EquipYPartitionKey> keyList = new ArrayList<EquipYPartitionKey>();
            List<EquipYPartitionValue> valueList = new ArrayList<EquipYPartitionValue>();
            
            
            for (EquipYPartitionPair pair : values){
                
                beforeOptimal.put(new EquipYPartitionKey(pair.getKey()), new EquipYPartitionValue(pair.getValue()));
                revertLookup.put(new EquipYPartitionValue(pair.getValue()), new EquipYPartitionKey(pair.getKey()));
                
                //System.out.println("Key: " + pair.getKey() + " Value: " + pair.getValue());
                
                keyList.add(new EquipYPartitionKey(pair.getKey()));
                valueList.add(new EquipYPartitionValue(pair.getValue()));
                
            }
            
            Collections.sort(keyList);
            Collections.sort(valueList, new EquipYPartitionValueComparator());
            
            int index = 0;
            
            if (beforeOptimal.size() <= dividerY){
              
                for (EquipYPartitionKey key : keyList){
                  context.write(new IntWritable(index++), beforeOptimal.get(key));
                }
              
            } else {
                
                for (int j = 0; j < (beforeOptimal.size() - dividerY); j++){
                    EquipYPartitionValue min = valueList.get(j);
                    EquipYPartitionKey resp = revertLookup.get(min);
                    
                    System.out.println("Get " + j + "th Minimum: " + "value: " + min + " key: " + resp);
                    int keyIndex = keyList.indexOf(resp);
                    int beforeNumber = 0;
                    int afterNumber = 0;

                    if (keyIndex - 1 < 0)
                        beforeNumber = Integer.MAX_VALUE;
                    else
                        beforeNumber = beforeOptimal.get(keyList.get(keyIndex - 1)).getNumber();
                    
                    if (keyIndex + 1 >= keyList.size())
                        afterNumber = Integer.MAX_VALUE;
                    else
                        afterNumber = beforeOptimal.get(keyList.get(keyIndex + 1)).getNumber();
                    
                    if ( beforeNumber <= afterNumber){
                        beforeOptimal.get(keyList.get(keyIndex - 1)).setNumber(beforeNumber + min.getNumber()); 
                        beforeOptimal.get(keyList.get(keyIndex)).setNumber(-1);
                    } else {
                        beforeOptimal.get(keyList.get(keyIndex + 1)).setNumber(beforeNumber + min.getNumber());
                        beforeOptimal.get(keyList.get(keyIndex)).setNumber(-1);
                    }
                }
                
                double hq = 0.0;
                
                for (EquipYPartitionKey key : keyList){
                    if (beforeOptimal.get(key).getNumber() >= 0){
                        context.write(new IntWritable(index), beforeOptimal.get(key));
                        writeResults.set(dividerY + ":" + String.valueOf(index), String.valueOf(beforeOptimal.get(key).getYEnd()));
                        System.out.println("current index is: " + key.toString() + " has " + beforeOptimal.get(key).getNumber() + " nodes.");
                        double tmp = (double)beforeOptimal.get(key).getNumber() / (double) nodeNumber;
                        hq += (tmp * Math.log(1 / tmp));
                        index++;
                    }
                }
                writeResults.set("HQ"+dividerY, String.valueOf(hq));
            }
        }
    }
}
