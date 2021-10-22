package hadoop.mapreduce.M07_Compress.Snappy;

import hadoop.mapreduce.M01_wordcount.WordCountDriver_v2;
import hadoop.mapreduce.M01_wordcount.WordCountMapper;
import hadoop.mapreduce.M01_wordcount.WordCountReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author: Suofen
 * description: TODO 在进行MR处理时的Shuffle阶段使用Snappy压缩一下在进行文件传输以
 *                   降低减少网络传输带宽，降低IO读写的次数
 * create time: TODO 2021/10/20 20:03
 *
  * @Param: null
 * @return
 */
public class ShuffleSnapy extends Configured implements Tool {

    public static void main(String[] args) throws Exception{
        //创建配置启动项
        Configuration conf = new Configuration();

        //todo 设置Map端输出压缩格式
        //todo Shuffle过程进行压缩，只有在输入的数据较大时才使用，否则小文件光压缩解压的时间都很浪费了
        conf.set("mapreduce.map.output.compress","true");
        //Lz4,lz0,Snappy,Gzip
        conf.set("mapreduce.map.output.compress.codec","org.apache.hadoop.io.compress.Lz4Codec");

        //使用工具类ToolRunner类提交程序
        int status = ToolRunner.run(conf, new ShuffleSnapy(),args);
        //退出客户端
        System.exit(status);
    }

    public int run(String[] args) throws Exception {
        //构建Job作业的实例
        Job job = Job.getInstance(getConf(), WordCountDriver_v2.class.getSimpleName());

        //TODO 设置job属性
        //设置MapReduce程序
        job.setJarByClass(WordCountDriver_v2.class);

        //设置本次MapReduce程序的mapper类Reduce类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReduce.class);

        //TODO 设置本次MapReduce程序的Combiner类 【！！慎重使用！！】
        job.setCombinerClass(WordCountReduce.class);

        /*todo Q:mapper阶段输入的Key和Value数据类型默认为
                 key:LongWritable
                 value:Text
        */
        //指定mapper阶段输出的Key和Value数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        /*todo Q:reducer阶段输入的Key和Value数据类型
                 就是mapper阶段输出的Key和Value数据类型
        */

        //指定reducer阶段输出的Key和Value数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //TODO Q设置MapReduce过程中ReduceTask（reduce阶段的工作者）的个数
        //     不设置默认为1
        //     设置为几个则输出的结果文件就有几个
        job.setNumReduceTasks(1);

        //配置本次作业的输入数据路径和输出数据路径
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        //todo 默认组件  FileInputFormat FileOutputFormat
        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);

        //TODO 判断输出路径是否已近存在，如果存在先删除
        FileSystem fs = FileSystem.get(getConf());
        if (fs.exists(output)) {
            fs.delete(output, true);//rm -rf
        }
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
