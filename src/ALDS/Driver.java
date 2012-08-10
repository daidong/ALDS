/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ALDS;

import ALDS.DataType.DataPair;
import ALDS.DataType.EquipYPartitionKey;
import ALDS.DataType.EquipYPartitionPair;
import ALDS.DataType.EquipYPartitionValue;
import ALDS.DataType.MIArray;
import ALDS.DataType.MIArrayWithKey;
import ALDS.DataType.MIVector;
import ALDS.DataType.MIVectorWithK;
import ALDS.PublicDataVisitor.DistMemCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 *
 * @author daidong
 */
public class Driver{
    
    public static int reducers;
    public static double maxX;
    public static double maxY;
    public static int nodes;
    
    public static void main(String[] args) throws Exception  {
        
        long time = System.currentTimeMillis();
        
        if (args.length != 6){
            System.out.println("Usage: Driver nodes reducers maxX maxY mode[random|linear|exp|period]");
            return;
        }
        
        Driver.nodes = Integer.parseInt(args[1]);
        Driver.reducers = Integer.parseInt(args[2]);
        Driver.maxX = Double.parseDouble(args[3]);
        Driver.maxY = Double.parseDouble(args[4]);
        String mode = args[5];
        String DFSInputFile = "InputDataSet.data";
        if (mode.equals("random"))
            DFSInputFile = "InputDataSet.data";
        if (mode.equals("linear"))
            DFSInputFile = "linearInputDataSet.data";
        if (mode.equals("exp"))
            DFSInputFile = "expInputDataSet.data";
        if (mode.equals("period"))
            DFSInputFile = "perInputDataSet.data";
        
        Configuration curJobConf = new Configuration();
        curJobConf.setInt("nodes", Driver.nodes);
        curJobConf.setInt("reducers", Driver.reducers);
        curJobConf.setStrings("maxX", args[3]);
        curJobConf.setStrings("maxY", args[4]);
        
        
        //////////////////////////////////////////////////////////
        Job YDistJob = new Job(curJobConf);
        YDistJob.setJarByClass(Driver.class);
        YDistJob.setJobName("EquiPartitionYAxisDist" + mode);

        FileInputFormat.addInputPath(YDistJob, new Path(DFSInputFile));
        FileOutputFormat.setOutputPath(YDistJob, new Path("ALDS/"+time+"-YDist.data"));

        YDistJob.setMapperClass(EquiPartitionYAxisDist.MapClassY1.class);
        YDistJob.setReducerClass(EquiPartitionYAxisDist.ReduceClassY1.class);
        YDistJob.setNumReduceTasks(Driver.reducers);

        YDistJob.setMapOutputKeyClass(IntWritable.class);
        YDistJob.setMapOutputValueClass(DataPair.class);
        YDistJob.setOutputKeyClass(EquipYPartitionKey.class);
        YDistJob.setOutputValueClass(EquipYPartitionValue.class);
        YDistJob.setOutputFormatClass(SequenceFileOutputFormat.class);

        YDistJob.waitForCompletion(true);
        
        //////////////////////////////////////////////////////////
        
        Job YJoinJob = new Job(curJobConf);
        YJoinJob.setJarByClass(Driver.class);
        YJoinJob.setJobName("EquiPartitionYAxisJoin" + mode);

        FileInputFormat.addInputPath(YJoinJob, new Path("ALDS/"+time+"-YDist.data"));
        FileOutputFormat.setOutputPath(YJoinJob, new Path("ALDS/"+time+"-YJoin.data"));

        YJoinJob.setMapperClass(EquiPartitionYAxisJoin.MapClassY2.class);
        YJoinJob.setReducerClass(EquiPartitionYAxisJoin.ReduceClassY2.class);
        YJoinJob.setNumReduceTasks(Driver.reducers);

        YJoinJob.setInputFormatClass(SequenceFileInputFormat.class);
        YJoinJob.setMapOutputKeyClass(IntWritable.class);
        YJoinJob.setMapOutputValueClass(EquipYPartitionPair.class);
        YJoinJob.setOutputKeyClass(IntWritable.class);
        YJoinJob.setOutputValueClass(EquipYPartitionValue.class);
        YJoinJob.setOutputFormatClass(SequenceFileOutputFormat.class);

        YJoinJob.waitForCompletion(true);
        
        DistMemCache distRead = new DistMemCache();
        
        for (int mapperId = 0; mapperId < 5; mapperId++) {
            distRead.set("NodeNumOf"+mapperId, String.valueOf(0));
        }
               
        //////////////////////////////////////////////////////////
        
        Job XDistJob = new Job(curJobConf);
        XDistJob.setJarByClass(Driver.class);
        XDistJob.setJobName("OptimalPartitionXAxisDist" + mode);

        FileInputFormat.addInputPath(XDistJob, new Path(DFSInputFile));
        FileOutputFormat.setOutputPath(XDistJob, new Path("ALDS/"+time+"-XDist.data"));

        XDistJob.setMapperClass(OptimalPartitionXAxisDist.MapClassX1.class);
        XDistJob.setReducerClass(OptimalPartitionXAxisDist.ReduceClassX1.class);
        XDistJob.setNumReduceTasks(Driver.reducers);

        XDistJob.setMapOutputKeyClass(IntWritable.class);
        XDistJob.setMapOutputValueClass(DataPair.class);
        XDistJob.setOutputKeyClass(Text.class);
        XDistJob.setOutputValueClass(MIArray.class);
        XDistJob.setOutputFormatClass(SequenceFileOutputFormat.class);

        XDistJob.waitForCompletion(true);
        
        
        //////////////////////////////////////////////////////////
        
        
        Job XJoinJob = new Job(curJobConf);
        XJoinJob.setJarByClass(Driver.class);
        XJoinJob.setJobName("OptimalPartitionXAxisJoin" + mode);

        FileInputFormat.addInputPath(XJoinJob, new Path("ALDS/"+time+"-XDist.data"));
        FileOutputFormat.setOutputPath(XJoinJob, new Path("ALDS/"+time+"-XJoin.data"));

        XJoinJob.setMapperClass(OptimalPartitionXAxisJoin.MapClassX2.class);
        XJoinJob.setReducerClass(OptimalPartitionXAxisJoin.ReduceClassX2.class);
        XJoinJob.setNumReduceTasks(Driver.reducers);

        XJoinJob.setInputFormatClass(SequenceFileInputFormat.class);
        XJoinJob.setMapOutputKeyClass(IntWritable.class);
        XJoinJob.setMapOutputValueClass(MIArrayWithKey.class);
        XJoinJob.setOutputKeyClass(IntWritable.class);
        XJoinJob.setOutputValueClass(MIArray.class);
        XJoinJob.setOutputFormatClass(SequenceFileOutputFormat.class);

        XJoinJob.waitForCompletion(true);
        
    }

}
