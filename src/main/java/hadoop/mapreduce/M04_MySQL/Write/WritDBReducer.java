package hadoop.mapreduce.M04_MySQL.Write;

import hadoop.mapreduce.M04_MySQL.Beans.UsaBean;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author: Suofen
 * description: TODO  在使用DBOutputFormat的时候
 *                    要求输出的Key必须是DBWritable的实现
 *                    因为只会吧Key写入到数据库
 * create time: TODO 2021/10/7 21:37
 *
  * @Param: null
 * @return
 */

public class WritDBReducer extends Reducer<NullWritable, UsaBean,UsaBean,NullWritable> {
    @Override
    protected void reduce(NullWritable key, Iterable<UsaBean> values, Context context) throws IOException, InterruptedException {
        for(UsaBean va :values){
            context.write(va,key);
        }
    }
}
