package hadoop.mapreduce.M06_IO.MapFile.ZhuanHua;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author: Suofen
 * description: TODO 实现TextFile-->MapFile
 * create time: TODO 2021/10/18 18:53
 *
  * @Param: null
 * @return
 */
public class MapFile_to_TextFile extends Configured implements Tool {
    //Mapper方法
    public static class ZhuanHuaMapper extends Mapper<IntWritable,Text, NullWritable,Text>{
        @Override
        protected void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(NullWritable.get(),value);
        }
    }

    //Reducer方法
    public static class ZhuanHuaReducer extends Reducer<NullWritable,Text,NullWritable,Text>{
        @Override
        protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iterator=values.iterator();
            while (iterator.hasNext()){
                context.write(key,iterator.next());
            }
        }
    }


    public static void main(String[] args) throws Exception{
        //创建配置启动项
        Configuration conf = new Configuration();

        //TODO：这个conf.set设置为local主要是为了用于本地测试
        //TODO：设置MapReduce程序的运行模式  如果不指定 默认为 local模式也就是本地，Window文件系统
        //conf.set("mapreduce.framework.name","local");
        //这句话其实可以不写，因为conf.set()方法的底层默认就是local

        //todo 使用工具类ToolRunner类提交程序
        int status = ToolRunner.run(conf, new MapFile_to_TextFile(),args);
        //退出客户端
        System.exit(status);
    }

    public int run(String[] args) throws Exception{
        //构建Job作业的实例
        Job job=Job.getInstance(getConf(), MapFile_to_TextFile.class.getSimpleName());

        //TODO 设置job属性
        //设置MapReduce程序
        job.setJarByClass(MapFile_to_TextFile.class);

        //设置本次MapReduce程序的mapper类Reduce类
        job.setMapperClass(ZhuanHuaMapper.class);
        job.setReducerClass(ZhuanHuaReducer.class);

        //指定mapper阶段输出的Key和Value数据类型
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        //指定reducer阶段输出的Key和Value数据类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);


        //TODO 设置输入文件的格式为SequenceFile类型，因为hadoop没有封装MapFileInputFormat
        job.setInputFormatClass(SequenceFileInputFormat.class);

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
