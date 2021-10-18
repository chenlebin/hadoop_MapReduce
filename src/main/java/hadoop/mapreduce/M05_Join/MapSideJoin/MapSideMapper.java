package hadoop.mapreduce.M05_Join.MapSideJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * @author: Suofen
 * description: TODO �ڷֲ�ʽ�����м�������
 * create time: TODO 2021/10/13 20:07
 *
  * @Param: null
 * @return
 */
public class MapSideMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    //todo  ����ȫ�ֱ���
    //�����������ڻ���Usa_sc����
    //�����ֵ�
    Map<String,String> UsascMap = new HashMap<String,String>();

    Text outkey = new Text();

    @Override
    /**
     * @author: Suofen
     * description: TODO �ڷֲ�ʽ�����ļ��м������ݣ����������Լ������ļ����У�����map������join����
     * create time: TODO 2021/10/13 20:11
     *
      * @Param: context
     * @return void
     */
    //todo ģ�����
    protected void setup(Context context) throws IOException, InterruptedException {
        //��ȡ�ֲ�ʽ�����ļ�  ע�⣺���ڻ����ļ���·��ֱ��  ָ���ļ�������   ����Ҫ���·��
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("usa_sc.txt")));
        String line = null;
        while((line = br.readLine())!=null){
            //������ʽ��
            //Alabama,Autauga
            //   ��  ,   ��
            String[] fields = line.split(",");
            //todo �ڳ�ʼ�������оͶ�ȡ���ֲ�ʽ�����ļ��е����ݲ�ƴ��
            //�Ѷ�ȡ�ķֲ�ʽ����������ӵ�������
            UsascMap.put(fields[1],fields[0]); //key����Ʒ���سǣ�V����
        }
    }

    @Override
    /**
     * @author: Suofen
     * description: TODO
     * create time: TODO 2021/10/13 20:21
     *
     * @Param: key
     * @Param: value
     * @Param: context
     * @return void
     */
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] str = value.toString().split(",");
        //������ʽ��
        // 1,2020/1/25,Cook,1,0
        //uid,ʱ��,��,ȷ��,����

        //todo  join�ص� ��usa_sc��ȡ���ؽ��й������ҵ������Ϣ����ƴ�ӣ�Ҳ������ν��join����
        //�ڶ���Ԫ��Ϊ�أ����й�����ON����
        String usa_sc = UsascMap.get(str[2]);
        outkey.set(usa_sc+"\t"+value.toString());
        context.write(outkey,NullWritable.get());

    }
}
