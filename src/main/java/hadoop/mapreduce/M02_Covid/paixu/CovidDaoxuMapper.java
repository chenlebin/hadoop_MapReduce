package hadoop.mapreduce.M02_Covid.paixu;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author: Suofen
 * description: TODO K2：输出的Key：州
 *                   V2：输出的Value：封装后的对象包含两个属性：确诊病例数 死亡病例数
 * create time: TODO 2021/10/3 21:31
 *
 * @Param: null
 * @return
 */
public class CovidDaoxuMapper extends Mapper<LongWritable, Text, CovidDaoxuBean,Text> {

    Text outvalue= new Text();
    CovidDaoxuBean outkey= new CovidDaoxuBean();
    @Override
    /**
     * @author: Suofen
     * description: 实现map方法处理usa疫情数据
     * create time: TODO 2021/10/3 20:54
     *
     * @Param: key     输入的key默认就是偏移量：LongWritable
     * @Param: value   水的value默认就是文本内容：Text
     * @Param: context 上下文对象
     * @return void
     */
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String string = value.toString();
        //读取一行数据，转化为String数组
        String[] fields = string.split("\t");
        //提取数据 州 确诊数 死亡数
        outvalue.set(fields[0]);
        outkey.set(Long.parseLong(fields[fields.length-2]),Long.parseLong(fields[fields.length-1]));
        //输出结果
        context.write(outkey,outvalue);
    }
}
