package hadoop.mapreduce.M03_JiShuQi;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author: 开冲
 * description: WordCount Mapper类 对应Maptask
 * create time: 2021/10/9 10:57
 *
 * KEYIN：是map阶段输入KV类型中的K类型，使用默认组件是起始位置偏移量，所以数据类型为：LongWritable
 * VALUEIN：是map阶段输入KV类型中的V类型，使用默认组件下是每一行的内容，所以数据类型为：Text
 * todo MapReduce 有默认的读取组件：TextInputFormat
 * todo 它读数据是一行一行读数据 ，返回KV键值对
 * todo K:每一行起始位置的偏移量 通常无意义---long类型对应hadoop封装数据类型的LongWritable
 * todo V:这一行文本的内容---Sring类型对应hadoop封装数据类型的Text类型
 *
 * KEYIN：是map阶段输出KV类型中的K类型，跟业务相关，本需求中输出的为单词，因此为Text
 * VALUEIN：是map阶段输出KV类型中的V类型，跟业务相关，本需求中输出单词次数1，因此为LongWritable
 *
 * @Param: null
 */
public class WordCountJiShuQiMapper extends Mapper<LongWritable, Text,Text,LongWritable> {

    //代码优化：
    //todo <对象复用>，只创建一个对象，一直对他的值进行覆盖
    private Text outKey = new Text();
    private final static LongWritable outValue = new LongWritable(1);

    /**
     * @author: Suofen
     * description: TODO map方法是mapper阶段的核心方法，也是具体业务逻辑实现的方法
     * create time: TODO 2021/10/9 18:28
     *
     * 注意：该方法调用的次数和输入的KV键值对有关，每当TextInputFormat读取
     * 返回一个KV键值对，调用一次map方法进行业务处理
     * 默认情况下：map方法是基于行来处理数据的
     *
     *
     * @Param: key     todo map阶段输入的键key
     * @Param: value   todo map阶段输入的值value
     * @Param: context todo 上(map)下(reduce)文对象
     * @return void
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //TODO :从程序的上下文环境获取一个全局计数器
        //     指定（参数） 计数器所属组的名字  计数器的名字
        Counter counter=context.getCounter("mapreduce_zdy","WordCount_apple");


        //TODO：关键问题，本来我总体全对了但是就是因为重写方法时自动生成了一下
        // super.map导致一直报错，所以下次一定到记得删除这两句话：！！！很重要！！！
        // super.map(k,v,context)
        // super.reduce(k,v,context)
        // 报错 原因分析：
        //TODO:super.map(key, value, context);


        //todo 拿取一行数据转化String类
        //第一行 hello tom hello allen hello
        String line = value.toString();
        //todo 提取分隔符进行切割，生成一个数组
        //[hello,tom,hello,allen,hello]
        //注意 \\代表空白字符，s+代表任意个
        String[] words = line.split("\\s+");
        //遍历数组
        //word 单个单词，words单词数组
        for(String word: words){
            //TODO 计数器的使用
            // 如果单词为apple则自增1
            if("apple".equals(word))
                //计数器方法，给计数器增加指定的数值
                counter.increment(1);

            //todo 将key的值进行覆盖
            outKey.set(word);
            //todo 输出数据  把每个单词单词都标记1，输出结果为<单词,1>
            //context对象,上下文对象
            //上文（map）的输入为：key:word 和 value:1
            //下文（reduce）的接收结果为：<hello,1><tom,1><hello,1><allen,1><hello,1>
            //todo 粗略写法，会生成多个Text对象，占用大量堆资源，浪费大量时间
            //todo context.write(new Text(word),new LongWritable(1));
            context.write(outKey,outValue);
        }

    }
}
