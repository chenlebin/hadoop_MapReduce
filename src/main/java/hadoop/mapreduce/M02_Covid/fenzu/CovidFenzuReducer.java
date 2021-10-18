package hadoop.mapreduce.M02_Covid.fenzu;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
/**
 * @author: Suofen
 * description: TODO
 * create time: TODO 2021/10/5 14:08
 *
 * @Param: null
 * @return
 */
public class CovidFenzuReducer extends Reducer<CovidBean, NullWritable,CovidBean,NullWritable> {
    @Override
    /**
     * @author: Suofen
     * description: TODO
     * create time: TODO 2021/10/5 14:08
     *
     * @Param: key
     * @Param: values
     * @Param: context
     * @return void
     */
    protected void reduce(CovidBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //TODO 不遍历迭代器，此时的Key就是分组中的第一个KV键值对，也就是该
        //     州确诊病例数最多的Top1，如果循环10次则输出该州最多的Top10
        //将每个州前十的确诊病例输出
        int num = 0;
        for (NullWritable nu : values){
            if(num<1) {
                context.write(key, NullWritable.get());
                num++;
            }
            else
                //当num>N则退出循环
                return;
        }

    }
}
