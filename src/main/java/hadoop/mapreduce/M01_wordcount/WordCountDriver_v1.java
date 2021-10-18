package hadoop.mapreduce.M01_wordcount;

import hadoop.mapreduce.M03_JiShuQi.WordCountJiShuQiMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver_v1 {
    /**
     * @author: Suofen
     * description: TODO Q:该类就是MapReduce程序客户端驱动类  主要构造Job对象实例
     *                  指定各组件属性  包括：mapper  reducer类，输入输出的
     *                  数据路径，提交job作业   job.submit()
     * create time: TODO 2021/9/30 11:05
     * 
     * @Param: null
     */
    public static void main(String[] args) throws Exception {
        //创建配置对象
        Configuration conf = new Configuration();

        //TODO：这个conf.set设置为local主要是为了用于本地测试
        //TODO：设置MapReduce程序的运行模式  如果不指定 默认为 local模式也就是本地，Window文件系统
        conf.set("mapreduce.framework.name","local");
        //这句话其实可以不写，因为conf.set()方法的底层默认就是local


        //构建Job作业的实例
        Job job=Job.getInstance(conf, WordCountDriver_v1.class.getSimpleName());

        //TODO 设置job属性
        //设置MapReduce程序
        job.setJarByClass(WordCountDriver_v1.class);

        //设置本次MapReduce程序的mapper类Reduce类
        job.setMapperClass(WordCountJiShuQiMapper.class);
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
        FileInputFormat.setInputPaths(job,input);
        FileOutputFormat.setOutputPath(job,output);

        //TODO 判断输出路径是否已近存在，如果存在先删除
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(output)){
            fs.delete(output,true);//rm -rf
        }

        //最终提交本次job作业
        // job.submit();
        //采用waitForCompletion提价job，参数表示是否开启实时监视追踪作业的执行
        boolean resultflag=job.waitForCompletion(true);
        //退出成都  和job结果进行绑定
        System.exit(resultflag ? 0: 1);
    }
}
