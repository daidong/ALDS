/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ALDS.StandAlone;

import ALDS.DataType.DataPair;
import ALDS.DataType.DataPairXComparator;
import ALDS.DataType.DataPairYComparator;
import ALDS.PublicDataVisitor.DistMemCache;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

/**
 *
 * @author daidong
 */
public class ReduceMemXAxisDist {

    private int dividerY = 0;
    private int dividerX = 0;
    private int c = 1;
    private ArrayList<Double> Q = null;
    private ArrayList<Integer> XPart = null;
    private ArrayList<Integer> YPart = null;
    //int AllXPart[][][] = null;
    int aXPart[][] = null;
    int bXPart[][] = null;
    double I[][] = null;
    DistMemCache distRead = null;
    ArrayList<DataPair> points = null;
    ArrayList<DataPair> SortedByX = null;
    ArrayList<DataPair> SortedByY = null;
    int nodes = 0;
    int numsGrid[][] = null;
    public double MI[][] = null;
    int B = 0;
    public static final String DATA_FILE = "/Users/daidong/Documents/research/ALDS/Code/x.txt";

    public ReduceMemXAxisDist(int b) throws FileNotFoundException, IOException {
        this.points = new ArrayList<DataPair>();

        File df = new File(ReduceMemXAxisDist.DATA_FILE);

        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(df)));
        String s = null;

        SortedByX = new ArrayList<DataPair>();
        SortedByY = new ArrayList<DataPair>();

        while ((s = br.readLine()) != null) {
            double xp = Double.parseDouble(s.split(" ")[0]);
            double yp = Double.parseDouble(s.split(" ")[1]);
            points.add(new DataPair(xp, yp));

            this.SortedByX.add(new DataPair(xp, yp));
            this.SortedByY.add(new DataPair(xp, yp));
        }

        Collections.sort(this.SortedByX, new DataPairXComparator());
        Collections.sort(this.SortedByY, new DataPairYComparator());
        this.nodes = this.points.size();

        this.B = b;
        MI = new double[B / 2 + 1][B / 2 + 1];
        int xpart = B / 2 + 2;
        aXPart = new int[xpart][xpart];
        bXPart = new int[xpart][xpart];

        this.numsGrid = new int[xpart][xpart];

    }

    public void process(int ys) {

        this.dividerY = ys;
        this.dividerX = (int) (B / dividerY);
        this.Q = new ArrayList<Double>();
        
        //AllXPart = new int[dividerX + 1][dividerX + 1][dividerX + 1];
        
        for (int i = 0; i < B/2 + 2; i++){
            for (int j = 0; j < B/2+2; j++){
                this.aXPart[i][j] = this.bXPart[i][j] = 0;
            }
        }
        
        I = new double[dividerX + 1][dividerX + 1];
        this.XPart = new ArrayList<Integer>();
        this.YPart = new ArrayList<Integer>();

        constructEquiYPartition();

        GetSuperClumpsPartition();

        double HQ = this.calHQ();
        //distRead.set("HQ", String.valueOf(HQ));

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

            //this.AllXPart[t][2][0] = XPart.get(0);
            //this.AllXPart[t][2][1] = sMax;
            this.aXPart[t][0] = 0;
            this.aXPart[t][1] = sMax;

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
                    this.bXPart[t][p] = this.aXPart[sMax][p];
                    //this.AllXPart[t][l][p] = this.AllXPart[sMax][l-1][p];
                }
                this.bXPart[t][l - 1] = t;
                //this.AllXPart[t][l][l-1] = t;

                for (int p = 0; p < l; p++) {
                    this.aXPart[t][p] = this.bXPart[t][p];
                }
                //I[t][l] = max;
                I[t][l] = this.calHP2(t, l) - this.calHPQ2(t, l);
                //System.out.println("Max: " + max + " I[t][l]: " + I[t][l]);
                //System.out.println("Max: " + max + " Result: " + I[t][l]);
                //System.out.println("I["+t+"]["+l+"]: " + I[t][l]);
            }
        }

        //System.out.println("XPart: " + this.XPart.size() + " dividerX: " + this.dividerX);
        //System.out.println("YPart: " + this.YPart.size() + " dividerY: " + this.dividerY);
        //double v[] = new double[dividerX + 1];
        double v[] = new double[this.XPart.size()];
        double currGridMax = 0 - Double.MAX_VALUE;
        for (int i = 1; i < this.XPart.size(); i++) {
            v[i] = (this.calHPTotal(i) - this.calHPQTotal(i) + HQ) / Math.log(Math.min(dividerX, dividerY));
            if (v[i] > currGridMax) {
                currGridMax = v[i];
            }
            //double c = (I[dividerX][i] + HQ) / Math.log(Math.min(dividerX, dividerY));
            //v[i] = (I[dividerX][i] + HQ) / Math.log(Math.min(dividerX, dividerY));
            //System.out.println(i + ": " + v[i] + " compare: " + c);
        }

        MI[dividerX][dividerY] = currGridMax;
        /*
        for (int l = 2; l <= dividerX; l++) {
        System.out.print("Partition For " + l + " is: ");
        
        for (int j = 0; j < l; j++) {
        System.out.print(this.AllXPart[dividerX][l][j] + " ");
        }
        
        System.out.println("");
        }
         */

    }

    public void constructEquiYPartition() {
        int currRow = 0;
        int desiredRowSize = (int) Math.ceil(this.nodes / this.dividerY);
        int i = 0;
        int currRowSize = 0;
        //ArrayList<Double> Q1 = new ArrayList<Double>();
        //ArrayList<Integer> con = new ArrayList<Integer>();

        while (i < this.nodes) {
            int s = 0;
            double startY = this.SortedByY.get(i).getY();
            double currY = startY;

            while (currY == startY && ++i < this.nodes) {
                currY = this.SortedByY.get(i).getY();
                s++;
            }

            if (currRowSize != 0
                    && Math.abs(currRowSize + s - desiredRowSize) > Math.abs(currRowSize - desiredRowSize)) {
                currRow++;
                currRowSize = 0;

                int rowsLeft = dividerY - currRow;
                if (rowsLeft == 0) {
                    rowsLeft = 1;
                }
                desiredRowSize = (int) Math.ceil((this.nodes - i) / rowsLeft);
            }

            currRowSize += s;
            if (currRow >= this.YPart.size()) {
                this.Q.add(currY);
                this.YPart.add(i - 1);
            } else {
                this.Q.set(currRow, currY);
                this.YPart.set(currRow, i - 1);
            }
            //con.add(currRowSize);
            //this.YPart.add(currRowSize);
        }
        this.YPart.add(0, 0);
        this.Q.add(0, 0 - Double.MAX_VALUE);
    }

    /*
    public void GetEquipYPartition(int ys){
    
    int currRow = 0;
    int i = 0;
    for (i = 0; i < this.nodes; i++){
    
    while (currRow < this.Q.size() && this.SortedByY.get(i).getY() >= this.Q.get(currRow)){
    currRow++;
    this.YPart.add(i);
    }
    }
    }
     */
    /*
     * Possible Error Situation: 
     * lots of points have the same x-axis value
     * 
     * Points on the column are included in the ith partition.
     */
    public int GetSuperClumpsPartition() {

        int i = 0, j = 0;
        int currRow = 0;
        int currCol = 0;
        int count = 0;
        int currSize = 0;
        int desiredColSize = this.nodes / this.dividerX;

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
                    > Math.abs(currSize - desiredColSize))) {
                this.XPart.add(i);
                currCol++;
                currSize = count;
                desiredColSize = (this.nodes - i - 1) / ((this.dividerX - currCol) == 0 ? 1 : (this.dividerX - currCol));
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

    public double calHPTotal(int l) {

        double result = 0.0;
        int i = 0;
        int currXPart[] = new int[l + 1];
        for (i = 0; i < l; i++) {
            currXPart[i] = this.aXPart[dividerX][i];
            //currXPart[i] = this.AllXPart[dividerX][l][i];
        }
        currXPart[i] = this.XPart.size() - 1;

        double overall = this.nodes;

        for (i = 1; i <= l; i++) {
            double xth = (double) countBeforeX(currXPart[i]) - countBeforeX(currXPart[i - 1]);
            double tmp = xth / overall;
            if (tmp != 0) {
                result += tmp * Math.log(1 / tmp);
            }
        }
        return result;
    }

    public double calHPQTotal(int l) {
        double result = 0.0;

        int currXPart[] = new int[l + 1];
        int i = 0;
        for (i = 0; i < l; i++) {
            currXPart[i] = this.aXPart[dividerX][i];
            //currXPart[i] = this.AllXPart[dividerX][l][i];
        }
        currXPart[i] = this.XPart.size() - 1;

        double overall = this.nodes;

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
            currXPart[i] = this.aXPart[t][i];
            //currXPart[i] = this.AllXPart[t][l][i];
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
            currXPart[i] = this.aXPart[t][i];
            //currXPart[i] = this.AllXPart[t][l][i];
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

    public static void main(String[] args) throws FileNotFoundException, IOException {

        int NodeNumber = 1000 * 10;
        double factor = 0.6;
        int B = (int) Math.pow(NodeNumber, factor);

        /*
         * Construct the input data
        
        Random r = new Random();
        int MAX_VALUE = 1000;
        
        File df = new File(ReduceMemXAxisDist.DATA_FILE);
        FileWriter fw = new FileWriter(df);
        for (int i = 0; i < NodeNumber; i++){
        //int x = Math.abs(r.nextInt() % MAX_VALUE);
        //int y = Math.abs(r.nextInt() % MAX_VALUE);
        
        //int xp = x * MAX_VALUE + y;
        //int xp = x * (MAX_VALUE + 1) + y;
        double x = r.nextDouble() * MAX_VALUE;
        double y = x + r.nextDouble();
        double z = Math.pow(Math.E, x);
        //int z = xp / (MAX_VALUE +  1) + xp - (xp / (MAX_VALUE + 1)) * (MAX_VALUE + 1); 
        fw.append(x + " " + y + "\n");
        //fw.append("MYDATA," + z + "," + xp + "\n");
        }
        fw.flush();
        fw.close();
         */

        ReduceMemXAxisDist instance = new ReduceMemXAxisDist(B);
        for (int ys = 2; ys < B / 2; ys++) {
            System.out.println("Process " + ys);
            instance.process(ys);
        }

        for (int i = 0; i < B / 2; i++) {
            for (int j = 0; j < B / 2; j++) {
                if (instance.MI[i][j] != 0) {
                    System.out.println(i + ":" + j + ": " + instance.MI[i][j]);
                }
            }
        }

    }
}
