package hadoop.MRproject.select;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class Select extends Configured implements Tool {

    public static void main(String[] args) throws Exception{
        //创建配置启动项
        Configuration conf = new Configuration();

        //使用工具类ToolRunner类提交程序
        int status = ToolRunner.run(conf, new Select(),args);
        //退出客户端
        System.exit(status);
    }

    public int run(String[] args) throws Exception {
        //构建Job作业的实例
        Job job = Job.getInstance(this.getConf(), Select.class.getSimpleName());

        //TODO 设置job属性
        //设置MapReduce程序
        job.setJarByClass(Select.class);

        //设置本次MapReduce程序的mapper类Reduce类
        job.setMapperClass(SelectMapper.class);
        job.setReducerClass(SelectReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //设置最终输出类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);

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

    /**
     * @author: Suofen
     * description: TODO 还是经典的字符串拼接
     * create time: TODO 2021/10/24 20:00
     *
     * @Param: null
     * @return
     */
    static class SelectMapper extends Mapper<LongWritable,Text,Text,Text>{
        Text OutKey = new Text();
        Text OutValue = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(",");
            //数据样式：
            //1,zhangsan,18,beijing
            OutKey.set(split[3]);
            OutValue.set(split[0]+','+split[1]+','+split[2]);
            //测试语句
            //System.out.println(OutKey.toString()+OutValue.toString());
            context.write(OutKey,OutValue);
        }
    }


    /**
     * @author: Suofen
     * description: TODO 对key和value进行判断
     *                   where 地址 = '上海' and age > 25
     * create time: TODO 2021/10/24 20:03
     *
     * @Param: null
     * @return
     */
    static class SelectReducer extends Reducer<Text,Text,NullWritable,Text>{
        Text OutValue = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //1,zhangsan,18,beijing数据
            if(key.toString().equals("shanghai")){
                for (Text value:values) {
                    //使用Integer.parseInt()将String类型强转为int类型
                    String[] split = value.toString().split(",");
                    //测试语句
                    //System.out.println("=============================");
                    //System.out.println(split[2]);
                    if(Integer.parseInt(split[2])>25){
                        OutValue.set(value.toString()+","+key.toString());
                        context.write(NullWritable.get(),OutValue);
                    }
                }
            }
        }
    }
}
