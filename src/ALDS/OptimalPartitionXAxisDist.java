/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ALDS;

import ALDS.DataType.DataPair;
import ALDS.DataType.DataPairXComparator;
import ALDS.DataType.DataPairYComparator;
import ALDS.DataType.Divs;
import ALDS.DataType.MIArray;
import ALDS.DataType.MIVector;
import ALDS.DataType.Partition;
import ALDS.PublicDataVisitor.DistMemCache;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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
public class OptimalPartitionXAxisDist {

    public static class MapClassX1 extends Mapper<LongWritable, Text, IntWritable, DataPair> {

        @Override
        public void map(LongWritable x, Text y, Context context) throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();

            double maxX = Double.parseDouble(conf.get("maxX"));
            int reducers = conf.getInt("reducers", 5);

            double sliceInterval = maxX / reducers;

            String point = new String(y.getBytes());
            String pointX = point.split(" ")[0];
            String pointY = point.split(" ")[1];
            Double X = Double.parseDouble(pointX);
            Double Y = Double.parseDouble(pointY);
            int k = (int) Math.floor(Y / sliceInterval);

            context.write(new IntWritable(k), new DataPair(X, Y));
        }
    }

    public static class ReduceClassX1 extends Reducer<IntWritable, DataPair, Text, MIArray> {

        private int nodeNum = 0;
        private ArrayList<Double> Q = null;
        private ArrayList<Integer> XPart = null;
        private ArrayList<Integer> YPart = null;
        int AllXPart[][][] = null;
        double I[][] = null;
        int numsGrid[][] = null;
        DistMemCache distRead = null;
        ArrayList<DataPair> points = null;
        ArrayList<DataPair> SortedByX = null;
        ArrayList<DataPair> SortedByY = null;
        int nodes = 0;
        

        @Override
        public void reduce(IntWritable k, Iterable<DataPair> values, Context context) throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();

            nodeNum = conf.getInt("nodes", 100);
            int reducers = conf.getInt("reducers", 5);
            int B = (int) (Math.pow(nodeNum, 0.6));
            int dividerX = 0;
            int dividerY = 0;

            this.points = new ArrayList<DataPair>();
            this.nodes = this.points.size();

            for (DataPair point : values) {
                double xp = point.getX();
                double yp = point.getY();
                points.add(new DataPair(xp, yp));
            }

            SortedByX = new ArrayList<DataPair>();
            SortedByY = new ArrayList<DataPair>();

            for (DataPair dp : this.points) {
                DataPair xPair = new DataPair(dp.getX(), dp.getY());
                DataPair yPair = new DataPair(dp.getX(), dp.getY());
                this.SortedByX.add(xPair);
                this.SortedByY.add(yPair);
            }

            Collections.sort(this.SortedByX, new DataPairXComparator());
            Collections.sort(this.SortedByY, new DataPairYComparator());

            int xpart = B/reducers + 2;
            AllXPart = new int[xpart][xpart][xpart];
            I = new double[xpart][xpart];
            numsGrid = new int[xpart][xpart];

            for (dividerY = reducers; dividerY < B / reducers; dividerY++) {

                dividerX = (int) (B / dividerY);

                this.Q = new ArrayList<Double>();
                this.Q.add(0 - Double.MAX_VALUE);

                distRead = new DistMemCache();
                for (int start = 0; start < dividerY; start++) {
                    Object w = distRead.get(dividerY + ":" + String.valueOf(start));
                    if (w != null) {
                        this.Q.add(Double.parseDouble((String) w));
                    }
                }

                if (this.XPart == null)
                    this.XPart = new ArrayList<Integer>();
                else
                    this.XPart.clear();
                
                if (this.YPart == null)
                    this.YPart = new ArrayList<Integer>();
                else 
                    this.YPart.clear();

                this.Q.add(this.SortedByY.get(this.nodes - 1).getY());

                GetEquipYPartition(dividerY);
                GetSuperClumpsPartition(dividerY, dividerX);

                constructAllGrid();

                for (int t = 2; t < this.XPart.size(); t++) {
                    double max = 0.0;
                    max = 0 - Double.MAX_VALUE;
                    int sMax = 1;
                    for (int s = 1; s < t; s++) {
                        double tmp = this.calHPShrink(s, t) - this.calHPQShrink(s, t);
                        if (tmp >= max) {
                            max = tmp;
                            sMax = s;
                        }
                    }

                    this.AllXPart[t][2][0] = XPart.get(0);
                    this.AllXPart[t][2][1] = sMax;

                    //I[t][2] = max;
                    I[t][2] = this.calHP2(t, 2) - this.calHPQ2(t, 2);
                }

                for (int l = 3; l <= dividerX; l++) {
                    for (int t = l; t < this.XPart.size(); t++) {
                        int sMax = l - 1;
                        double max = 0 - Double.MAX_VALUE;
                        for (int s = l - 1; s < t; s++) {

                            double tmp = (double) this.countBeforeX(s) / (double) this.countBeforeX(t) * I[s][l - 1]
                                    - (double) (this.countBeforeX(t) - this.countBeforeX(s)) / (double) this.countBeforeX(t)
                                    * this.calHPQCol2(s, t);

                            if (tmp > max) {
                                max = tmp;
                                sMax = s;
                            }
                        }

                        for (int p = 0; p < l; p++) {
                            this.AllXPart[t][l][p] = this.AllXPart[sMax][l - 1][p];
                        }
                        this.AllXPart[t][l][l - 1] = t;

                        //I[t][l] = max;
                        I[t][l] = this.calHP2(t, l) - this.calHPQ2(t, l);
                        //System.out.println("Max: " + max + " I[t][l]: " + I[t][l]);
                        //System.out.println("Max: " + max + " Result: " + I[t][l]);
                        //System.out.println("I["+t+"]["+l+"]: " + I[t][l]);
                    }
                }

                for (int i = this.XPart.size(); i <= dividerX; i++) {

                    for (int d = 2; d <= dividerX; d++) {

                        for (int j = 0; j < this.XPart.size(); j++) {

                            this.AllXPart[i][d][j] = this.AllXPart[this.XPart.size() - 1][d][j];

                        }
                    }

                    for (int j = 0; j < this.XPart.size(); j++) {
                        I[i][j] = I[this.XPart.size() - 1][j];
                    }
                }

                System.out.println("XPart: " + this.XPart.size() + " dividerX: " + dividerX);
                System.out.println("YPart: " + this.YPart.size() + " dividerY: " + dividerY);

                double v[] = new double[this.XPart.size()];

                for (int i = 1; i < this.XPart.size(); i++) {
                    v[i] = this.calHPTotal(i, dividerX) - this.calHPQTotal(i, dividerX);
                    //v[i] = (I[dividerX][i] + HQ) / Math.log(Math.min(dividerX, dividerY));
                    //v[i] = I[dividerX][i];
                    System.out.println(i + ": " + v[i]);
                }

                MIArray mia = new MIArray(v);
                int W = k.get();
                String outputKey = dividerY + ":" + W;
                context.write(new Text(outputKey), mia);
            }
        }

        public void GetEquipYPartition(int ys) {

            int currRow = 0;
            int i = 0;
            for (i = 0; i < this.nodes; i++) {

                while (currRow < this.Q.size() && this.SortedByY.get(i).getY() >= this.Q.get(currRow)) {
                    currRow++;
                    this.YPart.add(i);
                }
            }
        }

        /*
         * Possible Error Situation: 
         * lots of points have the same x-axis value
         * 
         * Points on the column are included in the ith partition.
         */
        public int GetSuperClumpsPartition(int ys, int xs) {

            int i = 0, j = 0;
            int currRow = 0;
            int currCol = 0;
            int count = 0;
            int currSize = 0;
            int desiredColSize = this.nodes / xs;

            this.XPart.add(0);

            while (i < this.nodes) {
                j = i;

                while (this.SortedByX.get(i).getY() > this.Q.get(currRow)) {
                    currRow++;
                }

                while (j < this.nodes
                        && this.SortedByX.get(j).getY() <= this.Q.get(currRow)
                        && this.SortedByX.get(j).getY() > this.Q.get(currRow - 1)) {
                    j++;
                    count++;
                }

                if ((currSize != 0) && (Math.abs(currSize + count - desiredColSize)
                        >= Math.abs(currSize - desiredColSize))) {
                    this.XPart.add(i);
                    currCol++;
                    currSize = count;
                    desiredColSize = (this.nodes - i - 1) / ((xs- currCol) == 0 ? 1 : (xs - currCol));
                } else {
                    currSize += count;
                }

                i += count;

                count = 0;
                currRow = 0;
            }

            /**
             * It seems that this would be added by loop procedure. Plase make sure
             */
            if (XPart.get(XPart.size() - 1) != (this.nodes - 1)) {
                this.XPart.add(this.nodes - 1);
            }

            for (int p : XPart) {
                System.out.println("Part at: " + p + ": " + SortedByX.get(p));
            }
            return XPart.size();

        }

        public double calHQ() {
            double result = 0.0;
            for (int i = 0; i < this.YPart.size(); i++) {
                double tmp = (double) countYth(i) / (double) this.nodes;
                if (tmp != 0) {
                    result += tmp * Math.log(1 / tmp);
                }
            }
            return result;
        }

        public double calHP(int t, int l) {
            double result = 0.0;

            for (int i = 0; i < l; i++) {
                double tmp = (double) countXth(i) / (double) countBeforeX(t);
                if (tmp != 0) {
                    result += tmp * Math.log(1 / tmp);
                }
            }
            return result;
        }

        public double calHPQ(int t, int l) {
            double result = 0;
            for (int i = 0; i < l; i++) {
                for (int j = 0; j < this.YPart.size(); j++) {
                    double tmp = (double) countXYth(i, j) / (double) countBeforeX(t);
                    if (tmp != 0) {
                        result += tmp * Math.log(1 / tmp);
                    }
                }
            }
            return result;
        }

        public double calHPTotal(int l, int dividerX) {

            double result = 0.0;
            int i = 0;
            int currXPart[] = new int[l + 1];
            for (i = 0; i < l; i++) {
                currXPart[i] = this.AllXPart[dividerX][l][i];
            }
            currXPart[i] = dividerX;

            double overall = this.nodeNum;

            for (i = 1; i <= l; i++) {
                double xth = (double) countBeforeX(currXPart[i]) - countBeforeX(currXPart[i - 1]);
                double tmp = xth / overall;
                if (tmp != 0) {
                    result += tmp * Math.log(1 / tmp);
                }
            }
            return result;
        }

        public double calHPQTotal(int l, int dividerX) {
            double result = 0.0;

            int currXPart[] = new int[l + 1];
            int i = 0;
            for (i = 0; i < l; i++) {
                currXPart[i] = this.AllXPart[dividerX][l][i];
            }
            currXPart[i] = dividerX;

            double overall = this.nodeNum;

            for (i = 1; i <= l; i++) {
                for (int j = 1; j < this.YPart.size(); j++) {
                    double xyth = (double) countXYAccording(i, j, currXPart, l);
                    double tmp = xyth / overall;
                    if (tmp != 0) {
                        result += tmp * Math.log(1 / tmp);
                    }
                }
            }
            return result;
        }

        public double calHP2(int t, int l) {

            double result = 0.0;
            int i = 0;
            int currXPart[] = new int[l + 1];
            for (i = 0; i < l; i++) {
                currXPart[i] = this.AllXPart[t][l][i];
            }
            currXPart[i] = t;


            double overall = (double) countBeforeX(t);

            for (i = 1; i <= l; i++) {
                double xth = countBeforeX(currXPart[i]) - countBeforeX(currXPart[i - 1]);
                double tmp = xth / overall;
                if (tmp != 0) {
                    result += tmp * Math.log(1 / tmp);
                }
            }

            return result;
        }

        public double calHPQ2(int t, int l) {
            double result = 0.0;

            int currXPart[] = new int[l + 1];
            int i = 0;
            for (i = 0; i < l; i++) {
                currXPart[i] = this.AllXPart[t][l][i];
            }
            currXPart[i] = t;

            double overall = (double) countBeforeX(t);

            for (i = 1; i <= l; i++) {
                for (int j = 1; j < this.YPart.size(); j++) {
                    double xyth = (double) countXYAccording(i, j, currXPart, l);
                    double tmp = xyth / overall;
                    if (tmp != 0) {
                        result += tmp * Math.log(1 / tmp);
                    }
                }
            }
            return result;
        }

        public int countXYAccording(int col, int row, int[] xpart, int maxX) {

            if (col < 1 || col > maxX) {
                return 0;
            }
            if (row < 1 || row >= YPart.size()) {
                return 0;
            }

            int cur = xpart[col];
            int bef = xpart[col - 1];

            if (cur >= this.XPart.size()) {
                cur = this.XPart.size() - 1;
            }

            if (bef >= this.XPart.size()) {
                bef = this.XPart.size() - 1;
            }

            int startX = this.XPart.get(bef);
            int endX = this.XPart.get(cur);

            double startY = this.SortedByY.get(this.YPart.get(row - 1)).getY();
            double endY = this.SortedByY.get(this.YPart.get(row)).getY();

            int count = 0;

            for (int i = startX; i < endX; i++) {
                double curr = this.SortedByX.get(i).getY();
                if (curr > startY && curr <= endY) {
                    count++;
                }
            }

            return count;
        }

        public double calHPShrink(int s, int t) {
            double result = 0.0;
            double tmp1 = (double) countBeforeX(s) / (double) countBeforeX(t);
            double tmp2 = (double) (countBeforeX(t) - countBeforeX(s)) / (double) countBeforeX(t);
            if (tmp1 != 0) {
                result += tmp1 * Math.log(1 / tmp1);
            }
            if (tmp2 != 0) {
                result += tmp2 * Math.log(1 / tmp2);
            }
            return result;
        }

        public double calHPQShrink(int s, int t) {
            double result = 0.0;
            for (int i = 0; i < this.YPart.size(); i++) {
                int count1 = countBeforeXAndYth(s, i);
                int count2 = countBeforeXAndYth(t, i);

                double tmp1 = (double) count1 / (double) countBeforeX(t);
                double tmp2 = (double) (count2 - count1) / (double) countBeforeX(t);

                if (tmp1 != 0) {
                    result += tmp1 * Math.log(1 / tmp1);
                }
                if (tmp2 != 0) {
                    result += tmp2 * Math.log(1 / tmp2);
                }
            }
            return result;
        }

        public double calHPQCol(int s, int t) {
            double result = 0.0;
            int sum = this.countBeforeX(t) - this.countBeforeX(s);
            for (int i = 0; i < this.XPart.size(); i++) {
                double tmp = (double) this.countBeforeXAndYth(s, t) / (double) sum;
                if (tmp != 0) {
                    result += tmp * Math.log(1 / tmp);
                }
            }
            return result;
        }

        public double calHPQCol2(int s, int t) {
            double result = 0.0;
            int sum = this.countBeforeX(t) - this.countBeforeX(s);
            for (int i = 0; i < this.YPart.size(); i++) {
                double tmp = (double) (this.countBeforeXAndYth(t, i) - this.countBeforeXAndYth(s, i)) / (double) sum;
                //double tmp = (double) this.countXYth(s, i) / (double) sum;
                if (tmp != 0) {
                    result += tmp * Math.log(1 / tmp);
                }
            }
            return result;
        }

        //colth starts from 1 to XPart.size
        private int countXth(int colth) {
            if (colth < 1 || colth >= XPart.size()) {
                return 0;
            }
            return this.XPart.get(colth) - this.XPart.get(colth - 1);
        }

        //count elements number before colth
        private int countBeforeX(int colth) {
            if (colth < 1) {
                return 0;
            }
            if (colth >= XPart.size()) {
                return this.nodes;
            }
            return this.XPart.get(colth);
        }

        //count 
        private int countYth(int rowth) {
            if (rowth < 1 || rowth >= YPart.size()) {
                return 0;
            }
            return this.YPart.get(rowth) - this.YPart.get(rowth - 1);
        }

        

        private int countBeforeXAndYth(int col, int rowth) {
            int count = 0;
            for (int i = 1; i <= col; i++) {
                count += this.numsGrid[i][rowth];
            }
            return count;
        }

        private int countXYth(int colth, int rowth) {
            return this.numsGrid[colth][rowth];
        }

        private void constructAllGrid() {
            for (int i = 0; i < this.XPart.size(); i++) {
                this.numsGrid[i][0] = 0;
            }
            for (int j = 0; j < this.YPart.size(); j++) {
                this.numsGrid[0][j] = 0;
            }
            for (int i = 1; i < this.XPart.size(); i++) {
                for (int j = 1; j < this.YPart.size(); j++) {
                    int count = 0;

                    int startX = this.XPart.get(i - 1);
                    int endX = this.XPart.get(i);

                    double pointStartY = this.SortedByY.get(this.YPart.get(j - 1)).getY();
                    double pointEndY = this.SortedByY.get(this.YPart.get(j)).getY();

                    for (int k = startX; k < endX; k++) {
                        double curr = this.SortedByX.get(k).getY();
                        if (curr > pointStartY && curr <= pointEndY) {
                            count++;
                        }
                    }
                    this.numsGrid[i][j] = count;
                }
            }
        }
        
        /*
         private int countBeforeXAndYth2(int col, int rowth) {
            int count = 0;
            if (col < 1) {
                return 0;
            }
            if (col >= XPart.size()) {
                col = XPart.size() - 1;
            }

            if (rowth < 1 || rowth >= YPart.size()) {
                return 0;
            }

            int startX = this.XPart.get(0);
            int endX = this.XPart.get(col);

            double startY = this.SortedByY.get(this.YPart.get(rowth - 1)).getY();
            double endY = this.SortedByY.get(this.YPart.get(rowth)).getY();

            for (int i = startX; i < endX; i++) {
                double curr = this.SortedByX.get(i).getY();
                if (curr > startY && curr <= endY) {
                    count++;
                }
            }
            return count;
        }

        private int countXYth2(int colth, int rowth) {
            if (colth < 1 || colth >= XPart.size()) {
                return 0;
            }
            if (rowth < 1 || rowth >= YPart.size()) {
                return 0;
            }

            double startX = this.SortedByX.get(this.XPart.get(colth - 1)).getX();
            double endX = this.SortedByX.get(this.XPart.get(colth)).getX();

            double startY = this.SortedByY.get(this.YPart.get(rowth - 1)).getY();
            double endY = this.SortedByY.get(this.YPart.get(rowth)).getY();

            int count = 0;

            if (countXth(colth) < countYth(rowth)) {
                for (int i = this.XPart.get(colth - 1); i < this.XPart.get(colth); i++) {
                    if (this.SortedByX.get(i).getY() > startY
                            && this.SortedByX.get(i).getY() <= endY) {
                        count++;
                    }
                }
            } else {
                for (int i = this.YPart.get(rowth - 1); i < this.YPart.get(rowth); i++) {
                    if (this.SortedByY.get(i).getX() > startX
                            && this.SortedByY.get(i).getX() <= endX) {
                        count++;
                    }
                }
            }
            return count;
        }
        */
    }
}
