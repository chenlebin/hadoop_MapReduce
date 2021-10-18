package hadoop.mapreduce.M06_IO.MapFile.ZhuanHua;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author: Suofen
 * description: TODO ʵ��TextFile-->MapFile
 * create time: TODO 2021/10/18 18:53
 *
  * @Param: null
 * @return
 */
public class MapFile_to_TextFile extends Configured implements Tool {
    //Mapper����
    public static class ZhuanHuaMapper extends Mapper<IntWritable,Text, NullWritable,Text>{
        @Override
        protected void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(NullWritable.get(),value);
        }
    }

    //Reducer����
    public static class ZhuanHuaReducer extends Reducer<NullWritable,Text,NullWritable,Text>{
        @Override
        protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iterator=values.iterator();
            while (iterator.hasNext()){
                context.write(key,iterator.next());
            }
        }
    }


    public static void main(String[] args) throws Exception{
        //��������������
        Configuration conf = new Configuration();

        //TODO�����conf.set����Ϊlocal��Ҫ��Ϊ�����ڱ��ز���
        //TODO������MapReduce���������ģʽ  �����ָ�� Ĭ��Ϊ localģʽҲ���Ǳ��أ�Window�ļ�ϵͳ
        //conf.set("mapreduce.framework.name","local");
        //��仰��ʵ���Բ�д����Ϊconf.set()�����ĵײ�Ĭ�Ͼ���local

        //todo ʹ�ù�����ToolRunner���ύ����
        int status = ToolRunner.run(conf, new MapFile_to_TextFile(),args);
        //�˳��ͻ���
        System.exit(status);
    }

    public int run(String[] args) throws Exception{
        //����Job��ҵ��ʵ��
        Job job=Job.getInstance(getConf(), MapFile_to_TextFile.class.getSimpleName());

        //TODO ����job����
        //����MapReduce����
        job.setJarByClass(MapFile_to_TextFile.class);

        //���ñ���MapReduce�����mapper��Reduce��
        job.setMapperClass(ZhuanHuaMapper.class);
        job.setReducerClass(ZhuanHuaReducer.class);

        //ָ��mapper�׶������Key��Value��������
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        //ָ��reducer�׶������Key��Value��������
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);


        //TODO ���������ļ��ĸ�ʽΪSequenceFile���ͣ���Ϊhadoopû�з�װMapFileInputFormat
        job.setInputFormatClass(SequenceFileInputFormat.class);

        //���ñ�����ҵ����������·�����������·��
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        //todo Ĭ�����  FileInputFormat FileOutputFormat
        FileInputFormat.setInputPaths(job,input);
        FileOutputFormat.setOutputPath(job,output);

        //TODO �ж����·���Ƿ��ѽ����ڣ����������ɾ��
        FileSystem fs = FileSystem.get(getConf());
        if(fs.exists(output)){
            fs.delete(output,true);//rm -rf
        }
        return job.waitForCompletion(true) ? 0:1;
    }
}
