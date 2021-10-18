package hadoop.mapreduce.M05_Join.UsaReduceJoin.usa_scOut;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
/**
 * @author: Suofen
 * description: TODO 通过Reduce方法只取出分组中的第一条数据然后输出即可输出美国56个州中的县
 * create time: TODO 2021/10/11 12:45
 *
  * @Param: null
 * @return
 */
public class OutReducer extends Reducer<UsaScBean, NullWritable,UsaScBean, NullWritable> {
    @Override
    protected void reduce(UsaScBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key,NullWritable.get());
    }
}
