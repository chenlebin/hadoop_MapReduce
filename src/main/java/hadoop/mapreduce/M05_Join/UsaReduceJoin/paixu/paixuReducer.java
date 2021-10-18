package hadoop.mapreduce.M05_Join.UsaReduceJoin.paixu;

import hadoop.mapreduce.M05_Join.UsaReduceJoin.usa_scOut.UsaScBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author: Suofen
 * description: TODO ��Ϊ�����Ƕ�join����֮���������һ��ȫ��������ѣ�
 *                   ��������ֻ��Ҫ��Reducer�����������
 * create time: TODO 2021/10/12 10:07
 *
  * @Param: null
 * @return
 */
public class paixuReducer extends Reducer<UsaScBean, Text,UsaScBean,Text> {
    @Override
    protected void reduce(UsaScBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Text value:values){
            //System.out.println("Key:"+key+"\t\tValue:"+value);
            context.write(key,value);
        }
    }
}
