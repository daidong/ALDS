/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ALDS;

import ALDS.DataType.DataPair;
import ALDS.DataType.EquipYPartitionKey;
import ALDS.DataType.EquipYPartitionValue;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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
public class EquiPartitionYAxisDist {
    
    public static class MapClassY1 extends Mapper<LongWritable, Text, IntWritable, DataPair>{
        
        @Override
        public void map(LongWritable x, Text y, Context context) throws IOException, InterruptedException{
            
            Configuration conf = context.getConfiguration();

            double maxY = Double.parseDouble(conf.get("maxY"));
            int reducers = conf.getInt("reducers", 5);
            double sliceInterval = maxY / reducers;
            
            //System.out.println("In MapClassY1 " + maxY + ": " + reducers);

            String point = new String(y.getBytes());
            String pointX = point.split(" ")[0];
            String pointY = point.split(" ")[1];
            Double X = Double.parseDouble(pointX);
            Double Y = Double.parseDouble(pointY);
            
            int k = (int) Math.floor(Y/sliceInterval);
            
            //System.out.println("Get X: " + X + " Get Y: " + Y + " Get k: " + k );
            
            context.write(new IntWritable(k), new DataPair(X, Y));
            
        }
        
        
    }
    
    public static class ReduceClassY1 extends Reducer<IntWritable, DataPair, EquipYPartitionKey, EquipYPartitionValue>{
    //public static class ReduceClassY1 extends Reducer<IntWritable, DataPair, Text, Text>{
        
        @Override
        public void reduce(IntWritable k, Iterable<DataPair> values, Context context) throws IOException, InterruptedException{
            
            Configuration conf = context.getConfiguration();
            
            int dividerY = 0;
            int nodeNumber = conf.getInt("nodes", 100);
            int reducers = conf.getInt("reducers", 5);
            int B = (int) (Math.pow(nodeNumber, 0.6));
            
            int i = 0;
            
            ArrayList<DataPair> sortedPoints = new ArrayList<DataPair>();
            for (DataPair p : values){
                sortedPoints.add(i, new DataPair(p.getX(), p.getY()));
                i++;
            }
            Collections.sort(sortedPoints);
            
            ArrayList<DataPair> Q = new ArrayList<DataPair>();
            ArrayList<Integer> S = new ArrayList<Integer>();
            
            for (dividerY = reducers; dividerY < B/reducers; dividerY++){
                i = 0;
                int equalSliceSize = nodeNumber / dividerY;
                int currRow = 0;
                int desiredRowSize = equalSliceSize;
                int currRowSize = 0;
                int size = sortedPoints.size();
                Q.clear();
                S.clear();
                
                while (i < size) {
                    int s = 0;
                    double y0 = sortedPoints.get(i).getY();
                    double currY = y0;

                    while (currY == y0 && ++i < size) {
                        currY = sortedPoints.get(i).getY();
                        s++;
                    }

                    if (currRowSize != 0
                            && Math.abs(currRowSize + s - desiredRowSize) >= Math.abs(currRowSize - desiredRowSize)) {

                        currRow++;
                        currRowSize = 0;
                        desiredRowSize = (nodeNumber - i) / ((dividerY - currRow) == 0 ? 1 : (dividerY - currRow));

                    }

                    currRowSize += s;
                    if (currRow >= Q.size()) {
                        Q.add(new DataPair(0, currY));
                        S.add(currRowSize);
                    } else {
                        Q.set(currRow, new DataPair(0, currY));
                        S.set(currRow, currRowSize);
                    }
                }

                //System.out.println("Overall Rows: " + currRow);

                for (int j = 0; j < currRow; j++) {
                    EquipYPartitionKey eypk = new EquipYPartitionKey(dividerY, k.get(), j);
                    double ystart = 0.0;
                    if (j == 0) {
                        ystart = 0;
                    } else {
                        ystart = Q.get(j - 1).getY();
                    }
                    EquipYPartitionValue eypv = new EquipYPartitionValue(ystart, Q.get(j).getY(), S.get(j));
                    System.out.println("Write Partitions: " + eypk + " Value: " + eypv);
                    //context.write(new Text(eypk.toString()), new Text(eypv.toString()));
                    context.write(eypk, eypv);
                }
            }
        }
    }
}
