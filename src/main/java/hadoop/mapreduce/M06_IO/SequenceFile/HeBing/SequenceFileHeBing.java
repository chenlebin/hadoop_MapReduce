package hadoop.mapreduce.M06_IO.SequenceFile.HeBing;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.File;
import java.io.FileInputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class SequenceFileHeBing {
    //����ȫ�ֱ���
    private Configuration configuration = new Configuration();
    private List<String> smallFilePaths = new ArrayList<>();
    /**
     * @author: Suofen
     * description: TODO ���巽�������С�ļ�·��
     * create time: TODO 2021/10/17 21:21
     *
      * @Param: null
     * @return
     */
    public void addInputPath(String inputPath) throws Exception {
        File file = new File(inputPath);
        //�����һ��Ŀ¼�������Ŀ¼�е������ļ�
        if(file.isDirectory()){
            File[] files = FileUtil.listFiles(file);
            for (File sFile: files){
                smallFilePaths.add(sFile.getPath());
                System.out.println("���С�ļ�·����"+sFile.getPath());
            }
        }
        //�����һ���ļ�����ֱ���������ļ���
        else {
            smallFilePaths.add(file.getPath());
            System.out.println("���С�ļ�·����"+file.getPath());
        }
    }

    /**
     * @author: Suofen
     * description: TODO ��smallFilePaths�е�С�ļ�������ȡ��Ȼ�����ϲ���SeqFile������
     * create time: TODO 2021/10/18 10:19
     *
      * @Param: null
     * @return
     */
    public void mergeFile(String lj) throws Exception{
        SequenceFile.Writer.Option bigFile = SequenceFile.Writer.file(new Path(lj));
        SequenceFile.Writer.Option keyClass = SequenceFile.Writer.keyClass(Text.class);
        SequenceFile.Writer.Option valueClass = SequenceFile.Writer.valueClass(BytesWritable.class);
        //����writer
        SequenceFile.Writer writer = SequenceFile.createWriter(configuration,bigFile,keyClass,valueClass);
        //������ȡС�ļ������д��SequenceFile
        System.out.println(smallFilePaths);
        Text key= new Text();
        for (String path:smallFilePaths) {
            File file = new File(path);
            long fileSize = file.length();//��ȡ�ļ����ֽ�����С
            byte[] fileContent = new byte[(int)fileSize];
            FileInputStream inputStream = new FileInputStream(file);
            inputStream.read(fileContent,0,(int) fileSize);
            String md55tr = DigestUtils.md5Hex(fileContent);
            System.out.println("mergeС�ļ���"+path+",md5:"+md55tr);
            key.set(path);
            //���ļ�·����Ϊkey���ļ�������Ϊvalue,���뵽SequenceFile��
            //writer.append(key, new BytesWritable(fileContent));
            writer.append(key,new BytesWritable(fileContent));
        }

        writer.hflush();
        writer.close();
    }

    /**
     * @author: Suofen
     * description: TODO ��ȡ���ļ��е�С�ļ�
     * create time: TODO 2021/10/18 10:31
     *
      * @Param: null
     * @return
     */
    public void readMergedFile(String lj) throws Exception{
        SequenceFile.Reader.Option file = SequenceFile.Reader.file(new Path(lj));
        SequenceFile.Reader reader = new SequenceFile.Reader(configuration,file);
        Text key = new Text();
        BytesWritable value = new BytesWritable();
        while(reader.next(key,value)){
            byte[] bytes = value.copyBytes();
            String md5 = DigestUtils.md5Hex(bytes);
            String content = new String(bytes, Charset.forName("GBK"));
            System.out.println("��ȡ�ļ�:"+key+"md5:"+md5+",+content:"+content);
        }


    }


    /**
     * @author: Suofen
     * description: TODO main�������÷���ʵ�ֹ���
     * create time: TODO 2021/10/17 21:27
     *
      * @Param: null
     * @return
     */
    public static void main(String[] args) throws Exception {
        //������Ķ���ʵ��
        SequenceFileHeBing seq = new SequenceFileHeBing();
        //todo �ϲ�С�ļ�
        //seq.addInputPath("H:\\hadoop-3.1.4_MapReduce_local_output\\MapReduce����\\IO\\SequenceFile\\HeBing\\input");
        //seq.mergeFile("H:\\hadoop-3.1.4_MapReduce_local_output\\MapReduce����\\IO\\SequenceFile\\HeBing\\output\\bigfile.seq");

        //todo ��ȡ���ļ�
        seq.readMergedFile("H:\\hadoop-3.1.4_MapReduce_local_output\\MapReduce����\\IO\\SequenceFile\\HeBing\\output\\bigfile.seq");
    }
}
