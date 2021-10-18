package hadoop.mapreduce.M06_IO.ORCFile.ZhuanHua;

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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.orc.OrcConf;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcOutputFormat;

import java.io.IOException;

public class TextFile_to_ORCFile extends Configured implements Tool {
    //��ҵ����
    private static final String JOB_NAME = TextFile_to_ORCFile.class.getSimpleName();

    //�������ݵ��ֶ���Ϣ
    private static final String SCHEMA = "struct<id:string,date:string,county:string,cases:string,deaths:string>";

    public static void main(String[] args) throws Exception{
        //��������������
        Configuration conf = new Configuration();

        //TODO�����conf.set����Ϊlocal��Ҫ��Ϊ�����ڱ��ز���
        //TODO������MapReduce���������ģʽ  �����ָ�� Ĭ��Ϊ localģʽҲ���Ǳ��أ�Window�ļ�ϵͳ
        //conf.set("mapreduce.framework.name","local");
        //��仰��ʵ���Բ�д����Ϊconf.set()�����ĵײ�Ĭ�Ͼ���local

        //todo ʹ�ù�����ToolRunner���ύ����
        int status = ToolRunner.run(conf, new TextFile_to_ORCFile(),args);
        //�˳��ͻ���
        System.exit(status);
    }

    public int run(String[] args) throws Exception{
        //todo ����Schema
        OrcConf.MAPRED_OUTPUT_SCHEMA.setString(this.getConf(),SCHEMA);

        //����Job��ҵ��ʵ��
        Job job=Job.getInstance(this.getConf(),JOB_NAME);

        //TODO ����job����
        //����MapReduce����
        job.setJarByClass(TextFile_to_ORCFile.class);

        //���ñ���MapReduce�����mapper��Reduce��
        job.setMapperClass(ZhuanHuaMapper.class);

        //todo ������������������������Ͷ�����Ҫָ����Ϊ�����õ�ORCFileָ�������������

        //todo ��������ReduceTask����Ϊ0ֱ����map�����
        job.setNumReduceTasks(0);

        //�����ļ�����������ΪTextFile����ͨ�ı��ļ���
        job.setInputFormatClass(TextInputFormat.class);

        //TODO ��������ļ������� : OrcOutputFormat
        job.setOutputFormatClass(OrcOutputFormat.class);

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

    /**
     * @author: Suofen
     * description: TODO ��TextFile-->ORCFile
     * create time: TODO 2021/10/18 21:32
     *
      * @Param: null
     * @return
     */
    public static class ZhuanHuaMapper extends Mapper<LongWritable,Text,NullWritable, OrcStruct>{
        //���������Key
        private final NullWritable outputKey = NullWritable.get();
        //��ȡ�ֶ�������Ϣ
        private TypeDescription schema = TypeDescription.fromString(SCHEMA);
        //���������valueΪORCStruct����
        private final OrcStruct outputValue = (OrcStruct) OrcStruct.createValue(schema);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //��ÿһ�����ݽ����з֣��õ������ֶ�
            String[] fields = value.toString().split(",", 5);
            //�������ֶθ�ֵ��Value�е���
            outputValue.setFieldValue(0,new Text(fields[0]));
            outputValue.setFieldValue(1,new Text(fields[1]));
            outputValue.setFieldValue(2,new Text(fields[2]));
            outputValue.setFieldValue(3,new Text(fields[3]));
            outputValue.setFieldValue(4,new Text(fields[4]));
            //���Key:Value
            context.write(outputKey,outputValue);
        }
    }
}
