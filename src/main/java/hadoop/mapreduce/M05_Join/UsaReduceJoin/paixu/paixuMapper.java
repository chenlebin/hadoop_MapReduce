package hadoop.mapreduce.M05_Join.UsaReduceJoin.paixu;

import hadoop.mapreduce.M05_Join.UsaReduceJoin.usa_scOut.UsaScBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
/**
 * @author: Suofen
 * description: TODO �Խ���
 * create time: TODO 2021/10/12 9:49
 *
  * @Param: null
 * @return
 */
public class paixuMapper extends Mapper<LongWritable, Text, UsaScBean,Text> {
    //����ȫ�ֱ���
    UsaScBean outkey = new UsaScBean();
    Text outvalue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] str = value.toString().split("\t");
        // ������ʽ  Abbeville	13619	2020/3/26	4	0	South Carolina
        //               0         1        2       3   4          5
        outkey.set(str[5],str[0]);
        String s=new String();
        //���������ݶ�ʧ������length-�±�����������
        s = str[1]+"\t"+str[2]+"\t"+str[str.length-3]+"\t"+str[str.length-2];
        outvalue.set(s);
        context.write(outkey,outvalue);
    }
}
