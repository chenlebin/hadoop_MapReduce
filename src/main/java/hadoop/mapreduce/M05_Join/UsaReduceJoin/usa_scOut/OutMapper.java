package hadoop.mapreduce.M05_Join.UsaReduceJoin.usa_scOut;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
/**
 * @author: Suofen
 * description: TODO ��Ҫ���ǽ����ַ���ƴ�Ӳ���
 * create time: TODO 2021/10/11 12:24
 *
  * @Param: null
 * @return
 */
public class OutMapper extends Mapper<LongWritable, Text,UsaScBean, NullWritable> {
    //����ȫ�ֱ���
    UsaScBean outkey = new UsaScBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //���ݵ����ӣ�2020/5/19,Fremont,Wyoming,228,5
        String[] s=value.toString().split(",");
        //�����ַ���ƴ��
        outkey.set(s[2],s[1]);

        //�������
        context.write(outkey,NullWritable.get());
    }
}
