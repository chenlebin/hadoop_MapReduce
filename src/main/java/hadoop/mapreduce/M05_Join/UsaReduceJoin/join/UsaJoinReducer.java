package hadoop.mapreduce.M05_Join.UsaReduceJoin.join;

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
public class UsaJoinReducer extends Reducer<Text,Text,Text, Text> {
    //TODO 创建集合用于保存usa_zong数据和usa_datedr数据,便于后续join之后进行拼接数据
    //用来保存usa数据 2020/1/21,Snohomish,1,0
    //                  时间      县     确诊 死亡
    List usalist = new ArrayList<String>();

    //用来保存usa_datedr数据 Alabama,Baldwin
    //                       州      县
    List usa_datedrlist = new ArrayList<String>();

    //需要输出的数据类型
    Text outvalue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //遍历Values
        for (Text value : values) {
            //判断数据时usa_zong数据还是usa_datedr的数据
            if(value.toString().startsWith("usa_zong#")){
                String s = value.toString().split("#")[1];
                System.out.println(s);
                //添加到usa_zong集合
                usalist.add(s);
            }

            if(value.toString().startsWith("usa_sc#")){
                String s = value.toString().split("#")[1];
                System.out.println(s);
                //添加到商品集合中
                usa_datedrlist.add(s);
            }
        }

            //获取两个集合的长度
            int usasize = usalist.size();
            int usa_datedrsize = usa_datedrlist.size();
            System.out.println("usa_zong:"+usasize+"         "+"usa_sc:"+usa_datedrsize);

            for (int i = 0; i < usa_datedrsize; i++) {
                for (int j = 0; j < usasize; j++) {
                    //拼接之后的数据   2020/1/21,1,0,Canaforniya
                    outvalue.set(usalist.get(j)+"\t"+usa_datedrlist.get(i));
                    context.write(key,outvalue);
                }

            }
            //清空List序列
            usalist.clear();
            usa_datedrlist.clear();

    }
}
