package hadoop.mapreduce.M02_Covid.fenzu;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
/**
 * @author: Suofen
 * description: TODO
 * create time: TODO 2021/10/5 14:00
 *
 * @Param: null
 * @return
 */
public class CovidFenzuMapper extends Mapper<LongWritable, Text, CovidBean, NullWritable> {
    CovidBean outkey = new CovidBean();
    NullWritable outvalue=NullWritable.get();
    @Override
    /**
     * @author: Suofen
     * description: TODO
     * create time: TODO 2021/10/5 14:01
     *
     * @Param: key
     * @Param: value
     * @Param: context
     * @return void
     */
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] str = value.toString().split(",");
        outkey.set(str[2],str[1], Long.parseLong(str[3]));
        context.write(outkey,outvalue);
    }
}
