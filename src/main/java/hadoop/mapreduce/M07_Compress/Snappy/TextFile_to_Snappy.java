package hadoop.mapreduce.M07_Compress.Snappy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @author: Suofen
 * description: TODO 将文本文件进行Snappy压缩
 * create time: TODO 2021/10/20 19:37
 *
  * @Param: null
 * @return
 */
public class TextFile_to_Snappy extends Configured implements Tool {

    public static void main(String[] args) throws Exception{
        //创建配置启动项
        Configuration conf = new Configuration();

        //todo 设置压缩格式
        conf.set("mapreduce.output.fileoutputformat.compress","true");
        conf.set("mapreduce.output.fileoutputformat.compress.codec","org.apache.hadoop.io.compress.SnappyCodec");

        //使用工具类ToolRunner类提交程序
        int status = ToolRunner.run(conf, new TextFile_to_Snappy(),args);
        //退出客户端
        System.exit(status);
    }

    public int run(String[] args) throws Exception {
        //构建Job作业的实例
        Job job = Job.getInstance(this.getConf(), TextFile_to_Snappy.class.getSimpleName());

        //TODO 设置job属性
        //设置MapReduce程序
        job.setJarByClass(TextFile_to_Snappy.class);

        //设置本次MapReduce程序的mapper类Reduce类
        job.setMapperClass(ZhuanHuaMapper.class);

        //设置最终输出类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        //不需要Reduce阶段直接设置为0
        job.setNumReduceTasks(0);

        //配置本次作业的输入数据路径和输出数据路径
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        //默认组件  FileInputFormat FileOutputFormat
        FileInputFormat.setInputPaths(job, input);

        FileOutputFormat.setOutputPath(job, output);

        //判断输出路径是否已近存在，如果存在先删除
        FileSystem fs = FileSystem.get(getConf());
        if (fs.exists(output)) {
            fs.delete(output, true);//rm -rf
        }
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static class ZhuanHuaMapper extends Mapper<LongWritable, Text, NullWritable,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //直接输出每条语句
            context.write(NullWritable.get(),value);
        }
    }
}
