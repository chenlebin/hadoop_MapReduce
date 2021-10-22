package hadoop.mapreduce.M07_Compress.Snappy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @author: Suofen
 * description: TODO ���ı��ļ�����Snappyѹ��
 * create time: TODO 2021/10/20 19:37
 *
  * @Param: null
 * @return
 */
public class TextFile_to_Snappy extends Configured implements Tool {

    public static void main(String[] args) throws Exception{
        //��������������
        Configuration conf = new Configuration();

        //todo ����ѹ����ʽ
        conf.set("mapreduce.output.fileoutputformat.compress","true");
        conf.set("mapreduce.output.fileoutputformat.compress.codec","org.apache.hadoop.io.compress.SnappyCodec");

        //ʹ�ù�����ToolRunner���ύ����
        int status = ToolRunner.run(conf, new TextFile_to_Snappy(),args);
        //�˳��ͻ���
        System.exit(status);
    }

    public int run(String[] args) throws Exception {
        //����Job��ҵ��ʵ��
        Job job = Job.getInstance(this.getConf(), TextFile_to_Snappy.class.getSimpleName());

        //TODO ����job����
        //����MapReduce����
        job.setJarByClass(TextFile_to_Snappy.class);

        //���ñ���MapReduce�����mapper��Reduce��
        job.setMapperClass(ZhuanHuaMapper.class);

        //���������������
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        //����ҪReduce�׶�ֱ������Ϊ0
        job.setNumReduceTasks(0);

        //���ñ�����ҵ����������·�����������·��
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        //Ĭ�����  FileInputFormat FileOutputFormat
        FileInputFormat.setInputPaths(job, input);

        FileOutputFormat.setOutputPath(job, output);

        //�ж����·���Ƿ��ѽ����ڣ����������ɾ��
        FileSystem fs = FileSystem.get(getConf());
        if (fs.exists(output)) {
            fs.delete(output, true);//rm -rf
        }
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static class ZhuanHuaMapper extends Mapper<LongWritable, Text, NullWritable,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //ֱ�����ÿ�����
            context.write(NullWritable.get(),value);
        }
    }
}
