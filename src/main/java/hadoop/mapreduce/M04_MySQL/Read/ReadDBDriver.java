package hadoop.mapreduce.M04_MySQL.Read;

import hadoop.mapreduce.M04_MySQL.Read.ReadBeans.usa_zongBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author: Suofen
 * description: TODO
 * create time: TODO 2021/10/7 18:35
 *
  * @Param: null
 * @return
 */

public class ReadDBDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //配置文件对象
        Configuration conf = new Configuration();
        //TODO 配置当前JDBC信息
        DBConfiguration.configureDB(
                //配置
                conf,
                //JDBC类
                "com.mysql.jdbc.Driver",
                //数据库URL
                //?useUnicode=true&amp;characterEncoding=utf-8   这一段是保证编码不乱码
                //node03上的数据库 jdbc:mysql://node03:3306/db_df2?useUnicode=true&amp;characterEncoding=utf-8
                //本地数据库       jdbc:mysql://localhost:3306/db_df2?useUnicode=true&amp;characterEncoding=utf-8
                //todo 关键点，如果换一个需要读取的表则需要修改这里
                "jdbc:mysql://localhost:3306/db_df2?useUnicode=true&amp;characterEncoding=utf-8",

                //用户名
                "root",
                //密码
                "root"
        );

        //创建作业的job类
        Job job = Job.getInstance(conf, ReadDBDriver.class.getSimpleName());

        //设置本地MR程序的驱动类
        job.setJarByClass(ReadDBDriver.class);

        //j设置mapper类
        job.setMapperClass(ReadDBMapper.class);

        //设置程序最终输出的KV类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        //TODO 因为本需求中不需要Reduce阶段，所以吧ReduceTask的个数设置为0个
        job.setNumReduceTasks(0);

        //TODO 设置输入组件
        job.setInputFormatClass(DBInputFormat.class);
        //TODO 添加读取数据库相关操作
        DBInputFormat.setInput(
                //提交的MR任务
                job,
                //todo 设置数据类型<封装好的Bean>
                usa_zongBean.class, //todo 关键点，如果换一个需要读取的表则需要修改这里
                //SQL语句  查询出的结果作为输出的数据
                "select date,county,cases,deaths from usa_zong", //todo 关键点，如果换一个需要读取的表则需要修改这里
                //计数Count(主键)SQL语句
                "select count(uid) from usa_zong" //todo 关键点，如果换一个需要读取的表则需要修改这里
        );
        //指定文件的输出路径
        Path output = new Path(args[0]);
        FileOutputFormat.setOutputPath(job,output);

        //TODO 判断输出路径是否已近存在，如果存在先删除
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(output)){
            fs.delete(output,true);//相当于  rm -rf /**路径**/output
        }

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0:1);



    }
}
