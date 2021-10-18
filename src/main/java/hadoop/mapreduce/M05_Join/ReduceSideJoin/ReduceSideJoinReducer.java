package hadoop.mapreduce.M05_Join.ReduceSideJoin;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author: Suofen
 * description: TODO
 * create time: TODO 2021/10/10 10:05
 *
  * @Param: null
 * @return
 */
public class ReduceSideJoinReducer extends Reducer<IntWritable,Text,IntWritable, Text> {
    //TODO 创建集合用于保存usa_zong数据和usa_datedr数据,便于后续join之后进行拼接数据
    //用来保存usa数据 1,Arizona,Maricopa,1,0
    //           编号     州      县  确诊人数  死亡人数
    List usalist = new ArrayList<String>();

    //用来保存usa_datedr数据 1,2020/1/21,0.0
    //                     编号   时间  死亡率
    List usa_datedrlist = new ArrayList<String>();

    //需要输出的数据类型
    Text outvalue = new Text();

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //遍历Values
        for (Text value : values) {
            //判断数据时usa_zong数据还是usa_datedr的数据
            if(value.toString().startsWith("usa_zong#")){
                String s = value.toString().split("#")[1];
                //System.out.println(s);
                //添加到商品集合中
                usalist.add(s);
            }

            if(value.toString().startsWith("usa_datedr#")){
                String s = value.toString().split("#")[1];
                //System.out.println(s);
                //添加到商品集合中
                usa_datedrlist.add(s);
            }
        }

            //获取两个集合的长度
            int usasize = usalist.size();
            int usa_datedrsize = usa_datedrlist.size();
            System.out.println("usasize:"+usasize+"         "+"usa_datedrsize:"+usa_datedrsize);

            for (int i = 0; i < usa_datedrsize; i++) {
                for (int j = 0; j < usasize; j++) {
                    //拼接之后的数据   1,Maricopa,Arizona,1,0,2020/1/29,0%
                    outvalue.set(usalist.get(j)+","+usa_datedrlist.get(i));
                    context.write(key,outvalue);
                }

            }
            //清空List序列
            usalist.clear();
            usa_datedrlist.clear();

    }
}
