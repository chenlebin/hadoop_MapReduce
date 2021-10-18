package hadoop.mapreduce.M05_Join.SQLJoin;

import hadoop.mapreduce.M04_MySQL.Beans.UsaZongBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author: Suofen
 * description: TODO DBInputFormat类用于从SQL表读取数据，底层是一行一行的读取表中国的数据
 *                   返回<K,V>键值对
 *                   K：是LongWritable类型，表中数据的记录行号从0开始
 *                   V：是DBWritable类型，表示该行数据对应的对象类型
 * create time: TODO 2021/10/7 18:27
 *
  * @Param: null
 * @return
 */

public class SQLjoinMapper extends Mapper<LongWritable, UsaZongBean, NullWritable, Text> {
    //LongWritable outkey = new LongWritable();
    Text outvalue = new Text();
    @Override
    protected void map(LongWritable key, UsaZongBean value, Context context) throws IOException, InterruptedException {
        //其实就是将数据原封不动的写出去
        //outkey.set(key.get());
        outvalue.set(value.toString());
        context.write(NullWritable.get(),outvalue);
    }
}
