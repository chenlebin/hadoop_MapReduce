package hadoop.mapreduce.M02_Covid.sum;

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
public class CovidSumReducer extends Reducer<Text, CovidCountBean,Text,CovidCountBean> {
    CovidCountBean outvalue = new CovidCountBean();
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
    protected void reduce(Text key, Iterable<CovidCountBean> values, Context context) throws IOException, InterruptedException {
        //统计变量
        long totalCases = 0;
        long totalDeaths = 0;
        //遍历该州各个县的数据
        for (CovidCountBean value : values){
            totalCases += value.getCases();
            totalDeaths += value.getDeaths();
        }

        //赋值
        outvalue.set(totalCases,totalDeaths);
        //输出结果：key：州 value：求和之后的结果
        context.write(key,outvalue);
    }
}
