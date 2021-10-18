package hadoop.mapreduce.M05_Join.MapSideJoin;

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

import java.net.URI;

/**
 * @author: Suofen
 * description: TODO Q:使用工具ToolRunner提交MapReduce作业
 * create time: TODO 2021/9/30 15:02
 *
 * @Param: null
 * @return
*/
public class MapSideJoinDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception{
        //创建配置启动项
        Configuration conf = new Configuration();

        //TODO：这个conf.set设置为local主要是为了用于本地测试
        //TODO：设置MapReduce程序的运行模式  如果不指定 默认为 local模式也就是本地，Window文件系统
        //conf.set("mapreduce.framework.name","local");
        //这句话其实可以不写，因为conf.set()方法的底层默认就是local

        //todo 使用工具类ToolRunner类提交程序
        int status = ToolRunner.run(conf, new MapSideJoinDriver(),args);
        //退出客户端
        System.exit(status);
    }

    public int run(String[] args) throws Exception{
        //构建Job作业的实例
        Job job=Job.getInstance(getConf(), MapSideJoinDriver.class.getSimpleName());

        //TODO 设置job属性
        //设置MapReduce程序
        job.setJarByClass(MapSideJoinDriver.class);

        //设置本次MapReduce程序的mapper类Reduce类
        job.setMapperClass(MapSideMapper.class);

        /*todo Q:mapper阶段输入的Key和Value数据类型默认为
                 key:LongWritable
                 value:Text
        */
        //指定mapper阶段输出的Key和Value数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        /*todo Q:reducer阶段输入的Key和Value数据类型
                 就是mapper阶段输出的Key和Value数据类型
        */

        //指定最终阶段输出的Key和Value数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //TODO 在map端jion操作时已经不需要reduceTask了
       //       因为没有Reducer阶段所以直接把reducer个数设置为0
        job.setNumReduceTasks(0);


        //todo 添加缓存文件 Map端Join关联的关键
        // 参数Url是HDFS中的绝对路径
        job.addCacheFile(new URI("/MapReduce/mapjoin/cache/usa_sc.txt"));

        //配置本次作业的输入数据路径和输出数据路径
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        //todo 默认组件  FileInputFormat FileOutputFormat
        FileInputFormat.setInputPaths(job,input);
        FileOutputFormat.setOutputPath(job,output);

        //TODO 判断输出路径是否已近存在，如果存在先删除
        FileSystem fs = FileSystem.get(getConf());
        if(fs.exists(output)){
            fs.delete(output,true);//rm -rf
        }
        return job.waitForCompletion(true) ? 0:1;
    }
}
