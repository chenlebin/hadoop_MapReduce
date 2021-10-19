package hadoop.mapreduce.M06_IO.ORCFile.ZhuanHua;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcInputFormat;

import java.io.IOException;

public class ORCFile_to_TextFile extends Configured implements Tool {

    public static void main(String[] args) throws Exception{
        //创建配置启动项
        Configuration conf = new Configuration();

        //TODO：这个conf.set设置为local主要是为了用于本地测试
        //TODO：设置MapReduce程序的运行模式  如果不指定 默认为 local模式也就是本地，Window文件系统
        //conf.set("mapreduce.framework.name","local");
        //这句话其实可以不写，因为conf.set()方法的底层默认就是local

        //todo 使用工具类ToolRunner类提交程序
        int status = ToolRunner.run(conf, new ORCFile_to_TextFile(),args);
        //退出客户端
        System.exit(status);
    }

    public int run(String[] args) throws Exception{
        //构建Job作业的实例
        Job job=Job.getInstance(this.getConf(), ORCFile_to_TextFile.class.getSimpleName());

        //TODO 设置job属性
        //设置MapReduce程序
        job.setJarByClass(ORCFile_to_TextFile.class);

        //设置本次MapReduce程序的mapper类Reduce类
        job.setMapperClass(ZhuanHuaMapper.class);

        //todo 在这里甚至连输出的数据类型都不需要指定因为我们用的ORCFile指定了输出的类型

        //todo 这里设置ReduceTask个数为0直接用map端输出
        job.setNumReduceTasks(0);

        //todo 设置文件的输入类型为ORCFile
        job.setInputFormatClass(OrcInputFormat.class);

        //设置输出文件的类型 :TextFile
        job.setOutputFormatClass(TextOutputFormat.class);

        //配置本次作业的输入数据路径和输出数据路径
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        //todo 特别注意这里的输入文件是ORCFile所以不是默认组件  OrcInputFormat FileOutputFormat
        OrcInputFormat.setInputPaths(job,input);
        FileOutputFormat.setOutputPath(job,output);

        //TODO 判断输出路径是否已近存在，如果存在先删除
        FileSystem fs = FileSystem.get(getConf());
        if(fs.exists(output)){
            fs.delete(output,true);//rm -rf
        }
        return job.waitForCompletion(true) ? 0:1;
    }

    /**
     * @author: Suofen
     * description: TODO 将ORCFile-->TextFile
     * create time: TODO 2021/10/18 21:32
     *
      * @Param: null
     * @return
     */
    public static class ZhuanHuaMapper extends Mapper<NullWritable,OrcStruct,NullWritable, Text>{
        //构建输出的Key
        private  NullWritable outputKey = NullWritable.get();

        //构建输出的value为ORCStruct类型
        private  Text outputValue = new Text();

        @Override
        protected void map(NullWritable key, OrcStruct value, Context context) throws IOException, InterruptedException {
            this.outputValue.set(
                            value.getFieldValue(0).toString()+","+
                            value.getFieldValue(1).toString()+","+
                            value.getFieldValue(2).toString()+","+
                            value.getFieldValue(3).toString()+","+
                            value.getFieldValue(4).toString()
            );
            //输出Key:Value
            context.write(outputKey,outputValue);
        }
    }
}
