package hadoop.mapreduce.M04_MySQL.Sqoop;

import java.io.IOException;

public class SqoopDBDriver {
    /**
     * @author: Suofen
     * description: TODO 实现从一个数据库表数据导出到另一个数据库表的操作
     *                   例如从本地数据库usa表中导出数据到node03机器上MySQLusa表上的操作
     *                   亦或是node03机器上MySQLusa表导出数据到本地数据库usa表上的操作
     * create time: TODO 2021/10/9 14:32
     *
     * @Param: null
     * @return
     */
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        if(ReadDB.read("H:\\hadoop-3.1.4_MapReduce_local_output\\MySQL操作\\从MySQL导出")==1) {
            WriteDB.write("H:\\hadoop-3.1.4_MapReduce_local_output\\MySQL操作\\从MySQL导出");
        }

    }
}
