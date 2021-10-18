package hadoop.mapreduce.M06_IO.ORCFile.ZhuanHua;

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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.orc.OrcConf;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcOutputFormat;

import java.io.IOException;

public class TextFile_to_ORCFile extends Configured implements Tool {
    //作业名称
    private static final String JOB_NAME = TextFile_to_ORCFile.class.getSimpleName();

    //定义数据的字段信息
    private static final String SCHEMA = "struct<id:string,date:string,county:string,cases:string,deaths:string>";

    public static void main(String[] args) throws Exception{
        //创建配置启动项
        Configuration conf = new Configuration();

        //TODO：这个conf.set设置为local主要是为了用于本地测试
        //TODO：设置MapReduce程序的运行模式  如果不指定 默认为 local模式也就是本地，Window文件系统
        //conf.set("mapreduce.framework.name","local");
        //这句话其实可以不写，因为conf.set()方法的底层默认就是local

        //todo 使用工具类ToolRunner类提交程序
        int status = ToolRunner.run(conf, new TextFile_to_ORCFile(),args);
        //退出客户端
        System.exit(status);
    }

    public int run(String[] args) throws Exception{
        //todo 设置Schema
        OrcConf.MAPRED_OUTPUT_SCHEMA.setString(this.getConf(),SCHEMA);

        //构建Job作业的实例
        Job job=Job.getInstance(this.getConf(),JOB_NAME);

        //TODO 设置job属性
        //设置MapReduce程序
        job.setJarByClass(TextFile_to_ORCFile.class);

        //设置本次MapReduce程序的mapper类Reduce类
        job.setMapperClass(ZhuanHuaMapper.class);

        //todo 在这里甚至连输出的数据类型都不需要指定因为我们用的ORCFile指定了输出的类型

        //todo 这里设置ReduceTask个数为0直接用map端输出
        job.setNumReduceTasks(0);

        //设置文件的输入类型为TextFile（普通文本文件）
        job.setInputFormatClass(TextInputFormat.class);

        //TODO 设置输出文件的类型 : OrcOutputFormat
        job.setOutputFormatClass(OrcOutputFormat.class);

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

    /**
     * @author: Suofen
     * description: TODO 将TextFile-->ORCFile
     * create time: TODO 2021/10/18 21:32
     *
      * @Param: null
     * @return
     */
    public static class ZhuanHuaMapper extends Mapper<LongWritable,Text,NullWritable, OrcStruct>{
        //构建输出的Key
        private final NullWritable outputKey = NullWritable.get();
        //获取字段描述信息
        private TypeDescription schema = TypeDescription.fromString(SCHEMA);
        //构建输出的value为ORCStruct类型
        private final OrcStruct outputValue = (OrcStruct) OrcStruct.createValue(schema);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //将每一行数据进行切分，得到所有字段
            String[] fields = value.toString().split(",", 5);
            //将所有字段赋值给Value中的列
            outputValue.setFieldValue(0,new Text(fields[0]));
            outputValue.setFieldValue(1,new Text(fields[1]));
            outputValue.setFieldValue(2,new Text(fields[2]));
            outputValue.setFieldValue(3,new Text(fields[3]));
            outputValue.setFieldValue(4,new Text(fields[4]));
            //输出Key:Value
            context.write(outputKey,outputValue);
        }
    }
}
