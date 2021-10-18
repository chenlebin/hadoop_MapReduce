package hadoop.mapreduce.M05_Join.UsaReduceJoin.paixu;

import hadoop.mapreduce.M05_Join.UsaReduceJoin.usa_scOut.UsaScBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author: Suofen
 * description: TODO 因为我们是对join关联之后的数据做一个全局排序而已，
 *                   所以我们只需要在Reducer遍历输出即可
 * create time: TODO 2021/10/12 10:07
 *
  * @Param: null
 * @return
 */
public class paixuReducer extends Reducer<UsaScBean, Text,UsaScBean,Text> {
    @Override
    protected void reduce(UsaScBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Text value:values){
            //System.out.println("Key:"+key+"\t\tValue:"+value);
            context.write(key,value);
        }
    }
}
