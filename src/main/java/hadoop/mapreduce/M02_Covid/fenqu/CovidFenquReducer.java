package hadoop.mapreduce.M02_Covid.fenqu;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
/**
 * @author: Suofen
 * description: TODO  如果在Reduce阶段实在没有需要输出的数据可以用NullWritable来代替，指没有输出
 * create time: TODO 2021/10/5 9:25
 *
 * @Param: null
 * @return
 */

public class CovidFenquReducer extends Reducer<Text,Text,Text, NullWritable> {
    @Override
    /**
     * @author: Suofen
     * description: TODO
     * create time: TODO 2021/10/5 9:26
     *
     * @Param: key
     * @Param: values
     * @Param: context
     * @return void
     */

    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //遍历迭代器中的数据
        //赋值
        //Text str = null;
        for( Text str : values){
            //TODO : 特别注意这里的NulL不能直接写null赋值，
            // 要用NullWritable.get来赋值，因为在hadoop中null经过封装之后
            // 是NullWritable，直接写null hadoop无法识别
            context.write(str,NullWritable.get());
        }
    }
}
