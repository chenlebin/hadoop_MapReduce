package hadoop.mapreduce.M04_MySQL.Sqoop;

import hadoop.mapreduce.M04_MySQL.Beans.UsaBean;
import hadoop.mapreduce.M04_MySQL.Write.WritDBMapper;
import hadoop.mapreduce.M04_MySQL.Write.WritDBReducer;
import hadoop.mapreduce.M04_MySQL.Write.WriteDBDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class WriteDB {
    public static int write(String path) throws IOException, ClassNotFoundException, InterruptedException {
        //配置文件对象
        Configuration conf = new Configuration();
        //TODO 配置当前JDBC信息
        DBConfiguration.configureDB(
                //配置
                conf,
                //JDBC类
                "com.mysql.jdbc.Driver",
                //数据库URL
                //?useUnicode=true&amp;characterEncoding=utf-8  这一段是保证编码不乱码
                //node03上的数据库 jdbc:mysql://node03:3306/db_df2?useUnicode=true&amp;characterEncoding=utf-8
                //本地数据库       jdbc:mysql://localhost:3306/db_df2?useUnicode=true&amp;characterEncoding=utf-8
                "jdbc:mysql://localhost:3306/db_df2?useUnicode=true&amp;characterEncoding=utf-8",
                //用户名
                "root",
                //密码
                "root"
        );

        //创建作业的job类
        Job job = Job.getInstance(conf, WriteDBDriver.class.getSimpleName());

        //设置本地MR程序的驱动类
        job.setJarByClass(WriteDBDriver.class);

        //设置mapper类
        job.setMapperClass(WritDBMapper.class);

        //设置map阶段输出的KV类型
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(UsaBean.class);

        //设置reduce类
        job.setReducerClass(WritDBReducer.class);

        //设置程序最终输出的KV类型
        job.setOutputKeyClass(UsaBean.class);
        job.setOutputValueClass(NullWritable.class);

        //TODO 因为本需求中不需要Reduce阶段，所以吧ReduceTask的个数设置为0个
        job.setNumReduceTasks(1);

        //TODO 设置当前作业的输入文件路径
        Path input = new Path(path);
        FileInputFormat.setInputPaths(job,input);

        //TODO 设置程序的输出表
        job.setOutputFormatClass(DBOutputFormat.class);

        //TODO 配置当前作业写入数据库的表，字段

        DBOutputFormat.setOutput(
                job,
                "usa",
                "uid","state","county","cases","deaths"
        );


        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0:1);

        if(b){
            System.out.println("\n\n\n                  写入数据库操作完成\n\n\n");
            return 1;
        }
        else{
            System.out.println("\n\n\n                  写入数据库操作异常\n\n\n");
            return 0;
        }

    }
}
