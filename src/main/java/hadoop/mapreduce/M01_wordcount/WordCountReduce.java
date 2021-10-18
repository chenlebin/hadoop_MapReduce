package hadoop.mapreduce.M01_wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
/*
 * @author: Suofen
 * description: TODO 本类就是MapReduce阶段处理类 对应ReduceTask
 * create time: TODO 2021/9/29 21:18
 * KEYIN   表示reduce阶段输入KV中的K类型，对应map阶段的输出key，因此本需求中就是单词  Text
 * VALUEIN 表示reduce阶段输入KV中的V类型，对应map阶段的输出Value 因此本需求中就是单词次数 1 LongWritable
 * KEYOUT  表示reduce阶段输出KV中的K类型，跟业务相关 本需求中 还是单词 Text
 * VALUEOUT表示reduce阶段输出KV中的V类型，跟业务相关 本需求中 还是单词总次数 LongWritable
 * @Param: null
 * @return
 */
public class WordCountReduce extends Reducer<Text,LongWritable,Text,LongWritable> {
    private LongWritable outValue = new LongWritable();

    @Override
    /**
     * @author: Suofen
     * description: TODO
     * create time: TODO 2021/9/29 21:30
     *
     * @Param: key
     * @Param: values
     * @Param: context
     *
     * @return void
     *
     *
     *todo Q:注意当map的所有输出数据来到reduce之后，该如何调用reduce方进行处理呢？
     * 0:从map中输出到reduce的数据：
     * <hello,1><hadoop,1><hello,1><hello,1><hadoop,1>
     * 1：排序   规则，根据key的字典序进行排序  a~z
     * <hadoop,1><hadoop,1><hello,1><hello,1><hello,1>
     * 2：分组  规则，key相同的分为一组
     * <hadoop,1><hadoop,1>
     * <hello,1><hello,1><hello,1>
     * 3：分组之后之后，同一组的数据组成一个新的KV键值对，调用一次reduce方法 。
     * reduce方法是基于分组调用的，一个分组调用一次【这里有两个分组所以只调用了两次reduce方法】
     * 同一组中数组组成一个新的KV键值对
     * 新key： 该组共同的Key
     * 新Value：该组所有的Value组成的一个迭代器Iterable
     * <hadoop,1><hadoop,1>----><hadoop,Iterable[1,1]>
     * <hello,1><hello,1><hello,1>----><hello,Iterable[1,1,1]>
     * 以上步骤均由Reducer内部自动完成（hadoop开发人员定义好的）
     */
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        //TODO：关键问题，本来我总体全对了但是就是因为重写方法时自动生成了一下
        // super.reduce导致一直报错，所以下次一定到记得删除这两句话：！！！很重要！！！
        // super.map(k,v,context)
        // super.reduce(k,v,context)
        //TODO:super.reduce(key, values, context);

        //统计变量
        long count = 0;
        //遍历该数组的value3
        for(LongWritable value :values) {
            //累加计算总次数
            count += value.get();
        }
        //给outValue赋值
        outValue.set(count);

        //最终使用上下文对象输出结果
        context.write(key,outValue);

    }
}
