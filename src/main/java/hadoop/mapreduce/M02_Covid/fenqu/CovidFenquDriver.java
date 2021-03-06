package hadoop.mapreduce.M02_Covid.fenqu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author: Suofen
 * description: TODO Q:使用工具ToolRunner提交MapReduce作业
 * create time: TODO 2021/9/30 15:02
 *
 * @Param: null
 * @return
 */
public class CovidFenquDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception{
        //创建配置启动项
        Configuration conf = new Configuration();

        //TODO：这个conf.set设置为local主要是为了用于本地测试
        //TODO：设置MapReduce程序的运行模式  如果不指定 默认为 local模式也就是本地，Window文件系统
        //conf.set("mapreduce.framework.name","local");
        //这句话其实可以不写，因为conf.set()方法的底层默认就是local

        //todo 使用工具类ToolRunner类提交程序
        int status = ToolRunner.run(conf, new CovidFenquDriver(),args);
        //退出客户端
        System.exit(status);
    }

    public int run(String[] args) throws Exception{
        //构建Job作业的实例
        Job job=Job.getInstance(getConf(), CovidFenquDriver.class.getSimpleName());

        //TODO 设置job属性
        //设置MapReduce程序
        job.setJarByClass(CovidFenquDriver.class);

        //设置本次MapReduce程序的mapper类Reduce类
        job.setMapperClass(CovidFenquMapper.class);
        job.setReducerClass(CovidFenquReducer.class);

        //TODO 设置本次MapReduce程序的Combiner类 【！！慎重使用！！】
        //job.setCombinerClass(WordCountReduce.class);

        /*todo Q:mapper阶段输入的Key和Value数据类型默认为
                 key:LongWritable
                 value:Text
        */
        //指定mapper阶段输出的Key和Value数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        /*todo Q:reducer阶段输入的Key和Value数据类型
                 就是mapper阶段输出的Key和Value数据类型
        */

        //指定reducer阶段输出的Key和Value数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //TODO Q设置MapReduce过程中ReduceTask（reduce阶段的工作者）的个数
        //     不设置默认为1
        //     设置为几个则输出的结果文件就有几个
        job.setNumReduceTasks(56);

        //TODO 指定分区规则：
        job.setPartitionerClass(StateFenqu.class);

        //配置本次作业的输入数据路径和输出数据路径
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        //todo 默认组件  FileInputFormat FileOutputFormat
        FileInputFormat.setInputPaths(job,input);
        FileOutputFormat.setOutputPath(job,output);

        //TODO 判断输出路径是否已近存在，如果存在先删除
        FileSystem fs = FileSystem.get(getConf());
        if(fs.exists(output)){
            fs.delete(output,true);//相当于  rm -rf /**路径**/output
        }
        return job.waitForCompletion(true) ? 0:1;
    }
}
