package hadoop.mapreduce.M05_Join.UsaReduceJoin.usa_scOut;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OutScCompartor extends WritableComparator {
    //������UsaScBeanʵ��
    protected OutScCompartor(){
        super(UsaScBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        //����ת��
        UsaScBean aBean = (UsaScBean) a;
        UsaScBean bBean = (UsaScBean) b;
        //TODO : �������з����������Ҫǰ��������county��һ��
        //       ��Ӧ�÷�Ϊͬһ��
        //       compare ���� ���� 0 MapReduce����Ϊ������ȷ�Ϊһ�飬
        //       Ĭ�ϲ�����0����������ͬ����������Ϊһ��
        return aBean.getCounty().compareTo(bBean.getCounty());
    }
}
