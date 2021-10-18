package hadoop.mapreduce.M05_Join.UsaReduceJoin.join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author: Suofen
 * description: TODO outkey:Text: county
 *                   outvalue:Text:其他文本内容
 * create time: TODO 2021/10/11 18:32
 *
 * @Param: null
 * @return
 */
public class UsaJoinMapper extends Mapper<LongWritable, Text,Text,Text>{
    //todo 定义全局变量
    //将获取的文件名做为全局变量
    String filename = null;

    //输出的KV键值对
    Text outkey = new Text();
    Text outvalue = new Text();
    //拼接之后的字符串
    // StringBuilder 对象时专门用来作字符串拼接工作的
    StringBuilder sb = new StringBuilder();

    @Override
    /*
     * @author: Suofen
     * description: TODO Mapper类中的初始化方法  获取当前处理的切片所属的文件名称
     *                   获取文件名 的操作为模板代码
     * create time: TODO 2021/10/9 19:56
     *
     * @Param: context
     * @return void
     */
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取当前处理的切片
        FileSplit split=(FileSplit)context.getInputSplit();
        //获取切片说属的文件名称  也就是当前Task处理的数据是属于哪一个文件的
        filename = split.getPath().getName();
        System.out.println("\n\n\n"+"当前处理的文件是"+filename+"\n\n\n");
    }

    @Override
    /**
     * @author: Suofen
     * description: TODO
     * create time: TODO 2021/10/11 18:42
     *
     * @Param: key
     * @Param: value
     * @Param: context
     * @return void
     */
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //TODO 利用StringBuilder 对象 sb 对两个文件进行字符串拼接
        //每次调用map方法（一行调用一次）都需要清空StringBuilder 对象 sb
        sb.setLength(0);

        //切割处理读取的行数据
        String[] fields = value.toString().split(",");

        //TODO 判断当前处理的文件是哪一个？
        // TODO .contains()方法用来判断字符串中是否包含参数里的字符串
        //      对所属不同文件的切片进行不同的字符串拼接处理
        if(filename.contains("usa_zong")){
        //usa表  数据举例
            //1,2020/1/21,Snohomish,1,0
            //对切割的数据进行拼接
            sb.append(fields[0]).append("\t").append(fields[1]).append("\t").append(fields[3]).append("\t").append(fields[4]);
            //System.out.println(sb);
            outkey.set(fields[2]);
            //在outvalue之前加上一个文件标识  usa_zong#
            outvalue.set(sb.insert(0,"usa_zong#").toString());
            context.write(outkey,outvalue);
        }else if(filename.contains("usa_sc")){
        //usa_datedr  数据举例
            //   州  ，  县
            //Alabama,Autauga
            //对切割的数据进行拼接
            sb.append(fields[0]);
            //System.out.println(sb);
            outkey.set((fields[1]));
            //在outvalue之前加上一个文件标识  usa_datedr#
            outvalue.set(sb.insert(0,"usa_sc#").toString());
            context.write(outkey,outvalue);
        }

    }
}
