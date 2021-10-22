package hadoop.mapreduce.M07_Compress.Snappy;

import hadoop.mapreduce.M01_wordcount.WordCountDriver_v2;
import hadoop.mapreduce.M01_wordcount.WordCountMapper;
import hadoop.mapreduce.M01_wordcount.WordCountReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author: Suofen
 * description: TODO �ڽ���MR����ʱ��Shuffle�׶�ʹ��Snappyѹ��һ���ڽ����ļ�������
 *                   ���ͼ������紫���������IO��д�Ĵ���
 * create time: TODO 2021/10/20 20:03
 *
  * @Param: null
 * @return
 */
public class ShuffleSnapy extends Configured implements Tool {

    public static void main(String[] args) throws Exception{
        //��������������
        Configuration conf = new Configuration();

        //todo ����Map�����ѹ����ʽ
        //todo Shuffle���̽���ѹ����ֻ������������ݽϴ�ʱ��ʹ�ã�����С�ļ���ѹ����ѹ��ʱ�䶼���˷���
        conf.set("mapreduce.map.output.compress","true");
        //Lz4,lz0,Snappy,Gzip
        conf.set("mapreduce.map.output.compress.codec","org.apache.hadoop.io.compress.Lz4Codec");

        //ʹ�ù�����ToolRunner���ύ����
        int status = ToolRunner.run(conf, new ShuffleSnapy(),args);
        //�˳��ͻ���
        System.exit(status);
    }

    public int run(String[] args) throws Exception {
        //����Job��ҵ��ʵ��
        Job job = Job.getInstance(getConf(), WordCountDriver_v2.class.getSimpleName());

        //TODO ����job����
        //����MapReduce����
        job.setJarByClass(WordCountDriver_v2.class);

        //���ñ���MapReduce�����mapper��Reduce��
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReduce.class);

        //TODO ���ñ���MapReduce�����Combiner�� ����������ʹ�ã�����
        job.setCombinerClass(WordCountReduce.class);

        /*todo Q:mapper�׶������Key��Value��������Ĭ��Ϊ
                 key:LongWritable
                 value:Text
        */
        //ָ��mapper�׶������Key��Value��������
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        /*todo Q:reducer�׶������Key��Value��������
                 ����mapper�׶������Key��Value��������
        */

        //ָ��reducer�׶������Key��Value��������
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //TODO Q����MapReduce������ReduceTask��reduce�׶εĹ����ߣ��ĸ���
        //     ������Ĭ��Ϊ1
        //     ����Ϊ����������Ľ���ļ����м���
        job.setNumReduceTasks(1);

        //���ñ�����ҵ����������·�����������·��
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        //todo Ĭ�����  FileInputFormat FileOutputFormat
        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);

        //TODO �ж����·���Ƿ��ѽ����ڣ����������ɾ��
        FileSystem fs = FileSystem.get(getConf());
        if (fs.exists(output)) {
            fs.delete(output, true);//rm -rf
        }
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
