package hadoop.mapreduce.M05_Join.UsaReduceJoin.usa_scOut;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
/**
 * @author: Suofen
 * description: TODO ͨ��Reduce����ֻȡ�������еĵ�һ������Ȼ����������������56�����е���
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
