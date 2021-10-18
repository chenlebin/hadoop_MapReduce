package hadoop.mapreduce.M06_IO.SequenceFile.ZhuanHua;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
/**
 * @author: Suofen
 * description: TODO
 * create time: TODO 2021/10/17 19:17
 *
  * @Param: null
 * @return
 */
public class ZhuanHuaReducer extends Reducer<NullWritable, Text,NullWritable,Text> {
    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value:values) {
            context.write(NullWritable.get(),value);
        }
    }
}
