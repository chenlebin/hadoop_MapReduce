package hadoop.mapreduce.M05_Join.UsaReduceJoin.paixu;

import hadoop.mapreduce.M05_Join.UsaReduceJoin.usa_scOut.UsaScBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
/**
 * @author: Suofen
 * description: TODO 对进行
 * create time: TODO 2021/10/12 9:49
 *
  * @Param: null
 * @return
 */
public class paixuMapper extends Mapper<LongWritable, Text, UsaScBean,Text> {
    //定义全局变量
    UsaScBean outkey = new UsaScBean();
    Text outvalue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] str = value.toString().split("\t");
        // 数据样式  Abbeville	13619	2020/3/26	4	0	South Carolina
        //               0         1        2       3   4          5
        outkey.set(str[5],str[0]);
        String s=new String();
        //可能有数据丢失所以用length-下标来代替数字
        s = str[1]+"\t"+str[2]+"\t"+str[str.length-3]+"\t"+str[str.length-2];
        outvalue.set(s);
        context.write(outkey,outvalue);
    }
}
