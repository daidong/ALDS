/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ALDS;

import ALDS.DataType.DataPair;
import ALDS.DataType.EquipYPartitionKey;
import ALDS.DataType.EquipYPartitionValue;
import ALDS.DataType.MIArray;
import ALDS.DataType.MIArrayWithKey;
import ALDS.DataType.MIVector;
import ALDS.DataType.MIVectorWithK;
import ALDS.PublicDataVisitor.DistMemCache;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
/**
 *
 * @author daidong
 */
public class OptimalPartitionXAxisJoin {
 
    public static class MapClassX2 extends Mapper<Text, MIArray, IntWritable, MIArrayWithKey>{

        
        @Override
        public void map(Text k, MIArray v, Context context) throws IOException, InterruptedException{
            String inputKey = new String(k.getBytes());
            int dividerY = Integer.parseInt(inputKey.split(":")[0]);
            int w = Integer.parseInt(inputKey.split(":")[1]);
            
            context.write(new IntWritable(dividerY), new MIArrayWithKey(w, v));
            
        }
    }
    
    public static class ReduceClassX2 extends Reducer<IntWritable, MIArrayWithKey, IntWritable, MIArray>{
        
       
        private int dividerY = 0;
        private int dividerX = 0;
        private int mappers = 0;
        
        
        private boolean increase(int [] array){
            int c = 0;
            array[0] = array[0] + 1;
            for (int j = 0; j < array.length; j++){
                if (array[j] >= dividerX){
                    c++;
                    if (j + 1 < array.length){
                        array[j] = 1;                
                        array[j+1] = array[j+1] + 1;
                    }
                }
            }
            /*
            System.out.print("After Increase: ");
            
            for (int j = 0; j < array.length; j++){
                System.out.print(array[j]+" ");
            }
            
            System.out.println("");
            */
            if (c >= array.length)
                return true;
            
            return false;
        }
        
        @Override
        public void reduce(IntWritable k, Iterable<MIArrayWithKey> values, Context context) throws IOException, InterruptedException{

            Configuration conf = context.getConfiguration();
            
            dividerY = k.get();
            int nodeNum = conf.getInt("nodes", 100);
            dividerX = (int) (Math.pow(nodeNum, 0.6) / dividerY);
            mappers = conf.getInt("reducers", 5);
            
            double MaxMI[] = new double[(dividerX + 2)];
            double MI[][] = new double[dividerX + 2][mappers];
            
            for (int j = 0; j < (dividerX + 1) ; j++){
                MaxMI[j] = 0 - Double.MAX_VALUE;
            }
            
            for (int m = 0; m < mappers; m++){
                for (int n = 0; n <= dividerX; n++){
                    MI[n][m] = 0 - Double.MAX_VALUE;
                }
            }
    
            for (MIArrayWithKey every : values){
                double[] db = every.getArray().get();
                int kIndex = every.getKey();
                for (int j = 1; j < db.length; j++){
                    MI[j][kIndex] = db[j];
                }
            }
            /*
            for (int m = 0; m < mappers; m++){
                for (int n = 0; n <= dividerX; n++){
                    System.out.print(MI[n][m]+" ");
                }
                System.out.println("");
            }
            */
            int IndexArray[] = new int[mappers];
            for (int j = 0; j < mappers; j++)
                IndexArray[j] = 1;
            
            do{
                double sum = 0.0;
                int index = 0;
                
                for (int j = 0; j < mappers; j++){
                    sum += MI[IndexArray[j]][j];
                    index += IndexArray[j];
                }
                
                //if (index <= dividerX)
                //    System.out.println("current: " + index + " sum is " + sum + " MaxMI is " + MaxMI[index]);
                
                if (index <= dividerX && sum > MaxMI[index]){
                    MaxMI[index] = sum;
                    //System.out.println("Index: " + index + " maximum is " + sum);
                }
                
            } while (!increase(IndexArray));
            
            DistMemCache distRead = new DistMemCache();
            double hq = Double.parseDouble((String)distRead.get("HQ"+dividerY));
            
            double v[] = new double[dividerX + 1];
            for (int j = 0; j <= dividerX; j++)
                v[j] = 0;
            
            System.out.println("For DividerY: " + dividerY);
            for (int iIndex = mappers; iIndex <= dividerX; iIndex++) {
                v[iIndex] = (MaxMI[iIndex] + hq)/Math.log(Math.min(dividerX, dividerY));
                //v[iIndex] = MaxMI[iIndex];
                System.out.println(iIndex + ": " + v[iIndex]);
            }
            
            MIArray result = new MIArray(v);
            
            System.out.println("MaxMI for " + dividerY);
            
            context.write(new IntWritable(dividerY), result);
        }
    }
}
