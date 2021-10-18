package hadoop.mapreduce.M06_IO.SequenceFile.ZhuanHua;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ZhuanHuaMapper extends Mapper<LongWritable, Text, NullWritable,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.write(NullWritable.get(),value);
    }
}
