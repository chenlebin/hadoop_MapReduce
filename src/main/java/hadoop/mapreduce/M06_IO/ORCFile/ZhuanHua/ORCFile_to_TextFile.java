package hadoop.mapreduce.M06_IO.ORCFile.ZhuanHua;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcInputFormat;

import java.io.IOException;

public class ORCFile_to_TextFile extends Configured implements Tool {

    public static void main(String[] args) throws Exception{
        //��������������
        Configuration conf = new Configuration();

        //TODO�����conf.set����Ϊlocal��Ҫ��Ϊ�����ڱ��ز���
        //TODO������MapReduce���������ģʽ  �����ָ�� Ĭ��Ϊ localģʽҲ���Ǳ��أ�Window�ļ�ϵͳ
        //conf.set("mapreduce.framework.name","local");
        //��仰��ʵ���Բ�д����Ϊconf.set()�����ĵײ�Ĭ�Ͼ���local

        //todo ʹ�ù�����ToolRunner���ύ����
        int status = ToolRunner.run(conf, new ORCFile_to_TextFile(),args);
        //�˳��ͻ���
        System.exit(status);
    }

    public int run(String[] args) throws Exception{
        //����Job��ҵ��ʵ��
        Job job=Job.getInstance(this.getConf(), ORCFile_to_TextFile.class.getSimpleName());

        //TODO ����job����
        //����MapReduce����
        job.setJarByClass(ORCFile_to_TextFile.class);

        //���ñ���MapReduce�����mapper��Reduce��
        job.setMapperClass(ZhuanHuaMapper.class);

        //todo ������������������������Ͷ�����Ҫָ����Ϊ�����õ�ORCFileָ�������������

        //todo ��������ReduceTask����Ϊ0ֱ����map�����
        job.setNumReduceTasks(0);

        //todo �����ļ�����������ΪORCFile
        job.setInputFormatClass(OrcInputFormat.class);

        //��������ļ������� :TextFile
        job.setOutputFormatClass(TextOutputFormat.class);

        //���ñ�����ҵ����������·�����������·��
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        //todo �ر�ע������������ļ���ORCFile���Բ���Ĭ�����  OrcInputFormat FileOutputFormat
        OrcInputFormat.setInputPaths(job,input);
        FileOutputFormat.setOutputPath(job,output);

        //TODO �ж����·���Ƿ��ѽ����ڣ����������ɾ��
        FileSystem fs = FileSystem.get(getConf());
        if(fs.exists(output)){
            fs.delete(output,true);//rm -rf
        }
        return job.waitForCompletion(true) ? 0:1;
    }

    /**
     * @author: Suofen
     * description: TODO ��ORCFile-->TextFile
     * create time: TODO 2021/10/18 21:32
     *
      * @Param: null
     * @return
     */
    public static class ZhuanHuaMapper extends Mapper<NullWritable,OrcStruct,NullWritable, Text>{
        //���������Key
        private  NullWritable outputKey = NullWritable.get();

        //���������valueΪORCStruct����
        private  Text outputValue = new Text();

        @Override
        protected void map(NullWritable key, OrcStruct value, Context context) throws IOException, InterruptedException {
            this.outputValue.set(
                            value.getFieldValue(0).toString()+","+
                            value.getFieldValue(1).toString()+","+
                            value.getFieldValue(2).toString()+","+
                            value.getFieldValue(3).toString()+","+
                            value.getFieldValue(4).toString()
            );
            //���Key:Value
            context.write(outputKey,outputValue);
        }
    }
}
