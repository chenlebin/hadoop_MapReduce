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
 * description: TODO 在分布式缓存中加载内容
 * create time: TODO 2021/10/13 20:07
 *
  * @Param: null
 * @return
 */
public class MapSideMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    //todo  定义全局变量
    //创建集合用于缓存Usa_sc数据
    //创建字典
    Map<String,String> UsascMap = new HashMap<String,String>();

    Text outkey = new Text();

    @Override
    /**
     * @author: Suofen
     * description: TODO 在分布式缓存文件中加载内容，到本程序自己创建的集合中，便于map方法的join操作
     * create time: TODO 2021/10/13 20:11
     *
      * @Param: context
     * @return void
     */
    //todo 模板代码
    protected void setup(Context context) throws IOException, InterruptedException {
        //读取分布式缓存文件  注意：关于缓存文件的路径直接  指定文件名即可   不需要添加路径
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("usa_sc.txt")));
        String line = null;
        while((line = br.readLine())!=null){
            //数据样式：
            //Alabama,Autauga
            //   州  ,   县
            String[] fields = line.split(",");
            //todo 在初始化方法中就读取出分布式缓存文件中的内容并拼接
            //把读取的分布式缓存内容添加到集合中
            UsascMap.put(fields[1],fields[0]); //key是商品的县城，V是州
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
        //数据样式：
        // 1,2020/1/25,Cook,1,0
        //uid,时间,县,确诊,死亡

        //todo  join重点 在usa_sc中取出县进行关联，找到相关信息进行拼接，也就是所谓的join关联
        //第二号元素为县，进行关联的ON条件
        String usa_sc = UsascMap.get(str[2]);
        outkey.set(usa_sc+"\t"+value.toString());
        context.write(outkey,NullWritable.get());

    }
}
