package hadoop.mapreduce.M02_Covid.paixu;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author: Suofen
 * description: TODO
 * create time: TODO 2021/10/4 9:03
 *
 * @Param: null
 * @return
 */
public class CovidDaoxuReducer extends Reducer<CovidDaoxuBean,Text,Text,CovidDaoxuBean> {
    Text outkey = new Text();
    CovidDaoxuBean outvalue = new CovidDaoxuBean();
    @Override
    /**
     * @author: Suofen
     * description: TODO
     * create time: TODO 2021/10/4 9:03
     *
     * @Param: key
     * @Param: values
     * @Param: context
     * @return void
     */
    protected void reduce(CovidDaoxuBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        /* TODO 排序好之后之后reduce会自动进行分组规则：判断key是否相同，
                本案例当中，我们使用自定义对象，并且没有重写分组规则，所以默认比较对象的地址，导致每个KV对自己就都是一组
                例如：
                <CovidDaoxuBean,加州>--><CovidDaoxuBean,Iterable<加州>>
                <CovidDaoxuBean,德州>--><CovidDaoxuBean,Iterable<德州>>
        */

        //赋值
        outkey = values.iterator().next();
        outvalue = key;
        //输出结果：key：州 value：排序（倒序从大到小）之后的确诊死亡人数
        context.write(outkey,outvalue);
    }
}
