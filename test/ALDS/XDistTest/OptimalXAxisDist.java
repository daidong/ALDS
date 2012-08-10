/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ALDS.XDistTest;

import ALDS.DataType.DataPair;
import ALDS.DataType.DataPairXComparator;
import ALDS.DataType.Divs;
import ALDS.DataType.MIVector;
import ALDS.DataType.Partition;
import ALDS.PublicDataVisitor.DistMemCache;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

/**
 *
 * @author daidong
 */
public class OptimalXAxisDist {

    private int dividerY = 100;
    private int dividerX = (int) (Math.pow(1000 * 10, 0.9) / dividerY);
    private int c = 1;
    private ArrayList<Double> Q = null;
    private static Partition par;
    private static Divs P[][];
    private static Double I[][];
    DistMemCache distRead = null;
    ArrayList<DataPair> points = null;
    private int extraNode = 2000;

    public static void main(String[] args) throws FileNotFoundException, IOException{
        
        OptimalXAxisDist process = new OptimalXAxisDist();
        
        Random r = new Random();
        int MAX_VALUE = 1000;
        
        ArrayList<DataPair> v = new ArrayList<DataPair>();
        
        File df = new File("/Users/daidong/Documents/research/ALDS/Code/data.txt");
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream (df)));
        String s = null;
        while((s = br.readLine()) != null){
            double x = Double.parseDouble(s.split(" ")[0]);
            double y = Double.parseDouble(s.split(" ")[1]);
            v.add(new DataPair(x,y));
        }
        process.reduce(v);
        
    }
    
    
    public void reduce(ArrayList<DataPair> values) {

        int maxColSize = c * dividerX;
        int Q_size = dividerY;

        int sizeIndex = 1;
        points = new ArrayList<DataPair>();
        points.add(0, new DataPair(-1, -1));
        for (DataPair point : values) {
            points.add(sizeIndex, new DataPair(point));
            sizeIndex++;
        }

        Collections.sort(points, new DataPairXComparator());

        int LN = points.size() - 1;
        distRead = new DistMemCache();

        int PSize = Math.max(maxColSize + 1, Q_size + 1);
        par = new Partition(PSize + 1);
        I = new Double[dividerX + 1][dividerX + 1];
        P = new Divs[dividerX + 1][dividerX + 1];

        par.Q[0] = Double.MIN_VALUE;
        int start = 0;
        for (start = 0; start < dividerY; start++) {
            Object w = distRead.get(String.valueOf(start));
            if (w != null) {
                par.Q[start + 1] = Double.parseDouble((String) w);
                //Q.add(Double.parseDouble((String)w));
            }
        }
        par.Q[start] = Double.MAX_VALUE;


        for (int i = 0; i <= dividerX; i++) {
            for (int j = 0; j <= dividerX; j++) {
                P[i][j] = new Divs(PSize + 1);
            }
        }


        if (maxColSize <= 1) {
            return;
        }

        GetSuperclumpsPartition(LN, Q_size, maxColSize, points);

        double hq = HQ(Q_size, maxColSize);
        distRead.set("HQ", String.valueOf(hq));

        for (int t = 2; t <= maxColSize; t++) {
            double largest = 0;
            //double hq = HQ(Q_size, t);
            largest = HPcom(1, t) - HPQcom(1, t, Q_size); //calculate I(t,2)
            int sMax = 1;
            for (int s = 1; s <= t; s++) {
                double temp = HPcom(s, t) - HPQcom(s, t, Q_size);
                if (temp >= largest) {
                    largest = temp;
                    sMax = s;
                }
            }
            //P(t,2) I(t,2)
            P[t][2].Colacc[1] = par.Colacc[sMax];
            P[t][2].Colacc[2] = par.Colacc[t];
            P[t][2].Col[1] = par.Colacc[sMax];
            P[t][2].Col[2] = par.Colacc[t] - par.Colacc[sMax];
            for (int i = 1; i <= Q_size; i++) {
                P[t][2].box[i][1] = 0;
                P[t][2].box[i][2] = 0;
                for (int j = 1; j <= sMax; j++) {
                    P[t][2].box[i][1] += par.box[i][j];
                }
                for (int j = sMax + 1; j <= t; j++) {
                    P[t][2].box[i][2] += par.box[i][j];
                }
            }
            I[t][2] = largest;
        }

        for (int l = 3; l <= dividerX; l++) {
            for (int t = l; t <= maxColSize; t++) {
                //double hq = HQ(Q_size, t);
                int sMax = l - 1;
                double largest = Double.MIN_VALUE;
                for (int s = l - 1; s <= t; s++) {
                    double temp = (par.Colacc[s] + this.extraNode) / (par.Colacc[t] + this.extraNode ) * (I[s][l - 1]) - (par.Colacc[t] - par.Colacc[s]) / (par.Colacc[t] + this.extraNode) * HPQcol(s, t, Q_size);
                    //double temp = par.Colacc[s] / par.Colacc[t] * (I[s][l - 1]) - (par.Colacc[t] - par.Colacc[s]) / par.Colacc[t]  * HPQcol(s, t, Q_size);
                    if (temp > largest) {
                        largest = temp;
                        sMax = s;
                    }
                }
                //P(t,l)=P(s,l-1)U c(t)
                for (int i = 1; i <= l - 1; i++) {
                    P[t][l].Colacc[i] = P[sMax][l - 1].Colacc[i];
                    P[t][l].Col[i] = P[sMax][l - 1].Col[i];
                    for (int j = 1; j <= Q_size; j++) {
                        P[t][l].box[j][i] = P[sMax][l - 1].box[j][i];
                    }
                }
                P[t][l].Colacc[l] = par.Colacc[t];
                P[t][l].Col[l] = par.Colacc[t] - par.Colacc[sMax];
                for (int j = 1; j <= Q_size; j++) {
                    P[t][l].box[j][l] = 0;
                    for (int m = sMax + 1; m <= t; m++) {
                        P[t][l].box[j][l] += par.box[j][m];
                    }
                }

                //I(t,l)=HQ + HP + HPQ
                System.out.println("First: " + HP(P[t][l], t, l) + " Second: " + HPQ(P[t][l], t, l, Q_size));
                
                I[t][l] = HP(P[t][l], t, l) - HPQ(P[t][l], t, l, Q_size);
            }
        }

        System.out.println("HQ: " + hq);
        ArrayList<Double> v = new ArrayList<Double>();
        for (int iIndex = 2; iIndex <= dividerX; iIndex++) {
            //v.add((I[maxColSize][iIndex] + hq)/Math.log(Math.min(dividerX, dividerY)));
            v.add(I[maxColSize][iIndex]);
        }
        
        MIVector miv = new MIVector(v);
        System.out.println(" Miv: \n" + miv + "\n\nsize: " + v.size());
    }

    public void GetSuperclumpsPartition(int N, int Q_size, int k, ArrayList<DataPair> points) {
        int i = 1, j;
        int currCol = 1;
        int currRow = 1;
        int desiredColSize = N / k;
        int count = 0;
        int currSize = 0;
        par.P[0] = points.get(1).getX() - 0.5;
        par.Colacc[0] = 0;
        while (i <= N) {
            j = i;
            System.out.println("Point: " + points.get(i));
            while (points.get(i).getY() > par.Q[currRow]) {
                currRow++;
            }
            //System.out.println("currRow: " + currRow + " value: " + par.Q[currRow]);
            
            while (j <= N && points.get(j).getY() <= par.Q[currRow] && points.get(j).getY() > par.Q[currRow - 1]) {
                j++;
                count++;
            }
            if ((currSize != 0) && (Math.abs(currSize + count - desiredColSize) >= Math.abs(currSize - desiredColSize))) {
                par.P[currCol] = (double) (points.get(i - 1).getX() + points.get(i).getX()) / 2.0;
                par.Col[currCol] = currSize;
                par.Colacc[currCol] = par.Colacc[currCol - 1] + currSize;
                currCol++;
                desiredColSize = (N - i + 1) / (k - currCol + 1);
                currSize = count;
            } else {
                currSize += count;
            }
            par.box[currRow][currCol] += count;
            i += count;
            count = 0;
            currRow = 1;
        }
        par.P[currCol] = (double) points.get(N).getX() + 0.5;
        par.Col[currCol] = N - par.Colacc[currCol - 1];
        par.Colacc[currCol] = N;
        for (i = currCol + 1; i <= k; i++) {
            par.P[i] = par.P[currCol];
            par.Col[i] = 0;
            par.Colacc[i] = par.Colacc[currCol];
        }

    }

    public double HQ(int y, int t) {
            double sumAll = (double) par.Colacc[t];
            double result = 0;
            for (int i = 1; i <= y; i++) {
                double sumLine = 0;
                for (int j = 1; j <= t; j++) {
                    sumLine += (double) par.box[i][j];
                }
                if (sumLine != 0) {
                    double temp = (double) sumLine / (double) sumAll;
                    result += temp * Math.log(1 / temp);
                }
            }
            return result;
        }

        public double HP(Divs p, int t, int l) {
            double result = 0;
            for (int i = 1; i <= l; i++) {
                if (p.Col[i] != 0) {
                    double temp = (double) p.Col[i] / ((double) par.Colacc[t] + this.extraNode);
                    result += temp * Math.log(1 / temp);
                }
            }
            return result;
        }

        public double HPQ(Divs p, int t, int l, int y) {
            double result = 0;
            for (int i = 1; i <= l; i++) {
                for (int j = 1; j <= y; j++) {
                    if (p.box[j][i] != 0) {
                        double temp = (double) p.box[j][i] / ((double) par.Colacc[t] + this.extraNode);
                        result += temp * Math.log(1 / temp);
                    }
                }
            }
            return result;
        }

        double HPcom(int s, int t) {
            double result = 0;
            double temp1 = (double) par.Colacc[s] / ((double) par.Colacc[t] + this.extraNode);
            double temp2 = (double) (par.Colacc[t] - par.Colacc[s]) / ( (double) par.Colacc[t] + this.extraNode );
            if (temp1 != 0) {
                result += temp1 * Math.log(1 / temp1);
            }
            if (temp2 != 0) {
                result += temp2 * Math.log(1 / temp2);
            }
            return result;
        }

        double HPQcom(int s, int t, int y) {
            double result = 0;
            for (int i = 1; i <= y; i++) {
                int count1 = 0;
                int count2 = 0;
                for (int j = 1; j <= s; j++) {
                    count1 += par.box[i][j];
                }
                for (int j = s + 1; j <= t; j++) {
                    count2 += par.box[i][j];
                }
                if (count1 != 0) {
                    double temp = (double) count1 / ((double) (par.Colacc[t]) + this.extraNode);
                    result += temp * Math.log(1 / temp);
                }
                if (count2 != 0) {
                    double temp = (double) count2 / ( (double) (par.Colacc[t]) + this.extraNode);
                    result += temp * Math.log(1 / temp);
                }
            }
            return result;
        }

        double HPQcol(int s, int t, int y) {
            double result = 0;
            int sum = par.Colacc[t] - par.Colacc[s];
            for (int i = 1; i <= y; i++) {
                int count = 0;
                for (int j = s + 1; j <= t; j++) {
                    count += par.box[i][j];
                }
                if (count != 0) {
                    double temp = (double) count / (double) sum;
                    result += temp * Math.log(1 / temp);
                }
            }
            return result;
        }
}