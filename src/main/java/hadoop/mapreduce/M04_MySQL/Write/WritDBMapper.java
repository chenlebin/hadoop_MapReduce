package hadoop.mapreduce.M04_MySQL.Write;

import hadoop.mapreduce.M04_MySQL.Beans.UsaBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author: Suofen
 * description: TODO 读取文本数据写入到MYSQL中
 *                   在map阶段做读取文本数据
 *                   解析 封装成对象
 * create time: TODO 2021/10/7 21:20
 *
  * @Param: null
 * @return
 */
public class WritDBMapper extends Mapper<LongWritable,Text, NullWritable, UsaBean> {
    NullWritable outkey =NullWritable.get();
    UsaBean outvalue = new UsaBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取两个计数器 分别用于统计合法数据  非法数据
        Counter sc = context.getCounter("mapreduce_zdy","success");
        Counter fa = context.getCounter("mapreduce_zdy","failed");

        String[] fields = value.toString().split(",");
        //判断输入的数据长度是否够长  如果不满足需求 就是非法数据
        if(fields.length >= 5){
            //合法数据提取字段封装成对象的属性
            outvalue.set(
                    Integer.parseInt(fields[0]),
                    fields[1],
                    fields[2],
                    Integer.parseInt(fields[3]),
                    Integer.parseInt(fields[4])
            );
            //合法数据   计数器+1
            sc.increment(1);
            context.write(outkey,outvalue);

        }
        else{
            //非法数据   计数器+1
            fa.increment(1);
        }
    }
}
