package hadoop.mapreduce.M05_Join.UsaReduceJoin.usa_scOut;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
/**
 * @author: Suofen
 * description: TODO 主要还是进行字符串拼接操作
 * create time: TODO 2021/10/11 12:24
 *
  * @Param: null
 * @return
 */
public class OutMapper extends Mapper<LongWritable, Text,UsaScBean, NullWritable> {
    //定义全局变量
    UsaScBean outkey = new UsaScBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //数据的样子：2020/5/19,Fremont,Wyoming,228,5
        String[] s=value.toString().split(",");
        //进行字符串拼接
        outkey.set(s[2],s[1]);

        //输出数据
        context.write(outkey,NullWritable.get());
    }
}
