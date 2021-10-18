package hadoop.mapreduce.M02_Covid.fenqu;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
/**
 * @author: Suofen
 * description: TODO
 * create time: TODO 2021/10/5 9:18
 *
 * @Param: null
 * @return
 */
public class CovidFenquMapper extends Mapper<LongWritable,Text,Text,Text> {
    Text outkey= new Text();
    @Override
    /**
     * @author: Suofen
     * description: TODO
     * create time: TODO 2021/10/5 9:20
     *
     * @Param: key
     * @Param: value
     * @Param: context
     * @return void
     */
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //读取一行数据，转化为String数组
        String[] fields = value.toString().split(",");
        //提取数据 州 确诊数 死亡数
        outkey.set(fields[2]);
        context.write(outkey,value);
    }
}
