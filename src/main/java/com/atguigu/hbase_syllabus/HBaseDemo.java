package com.atguigu.hbase_syllabus;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2019/7/6.
 */
public class HBaseDemo {
    public static Configuration conf;
    static {
        conf = HBaseConfiguration.create();
    }

    /*判断表是否存在*/
    public  static  boolean  isExist(String tableName) throws IOException {
     /*HBase对表的操作的管理，都必须使用HBaseAdmin对象，该对象是通过HBase配置文件实例化的*/
     /*旧API兼容性好，可以找到新API的使用方法，不能太旧*/
      //  HBaseAdmin admin = new HBaseAdmin(conf);
      /*新API*/
        Connection conc = ConnectionFactory.createConnection(conf);
        Admin admin = conc.getAdmin();
       // HBase官方提供的标准二进制转化器
       // admin.tableExists(Bytes.toBytes(tableName));
        return admin.tableExists(TableName.valueOf(tableName));
    }

    //HBase表的创建
    public static void createTable(String tableName,String... clumnFamily) throws IOException {
        /*根据HBase配置文件创建HBase的连接*/
        Connection conc = ConnectionFactory.createConnection(conf);
        /*通过连接创建表管理对象*/
        Admin admin = conc.getAdmin();
        if (isExist(tableName)){
            System.out.println(tableName+"表已经存在！");
        }else{
             /*通过表名初始化表描述器*/
            HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
            for(String cf : clumnFamily){
            /*将列族添加到表描述器中*/
                htd.addFamily(new HColumnDescriptor(cf));
            }
        /*通过表描述器，完成表的创建*/
            admin.createTable(htd);
            System.out.println(tableName+"表创建成功！");
        }

    }

    /*HBase表的删除*/
    public static void deleteTable(String tableName) throws IOException {
        /*根据HBase配置文件创建HBase的连接*/
        Connection conc = ConnectionFactory.createConnection(conf);
        /*通过连接创建表管理对象*/
        Admin admin = conc.getAdmin();

        if(isExist(tableName)){
            /*如果这个表不是禁用状态*/
            if(!admin.isTableDisabled(TableName.valueOf(tableName))){
                /*那么就将这张表禁用*/
                admin.disableTable(TableName.valueOf(tableName));
            }
            /*禁用后的表才能被执行删除*/
            admin.deleteTable(TableName.valueOf(tableName));
            System.out.println(tableName+"表删除成功！");
        }else {
            System.out.println(tableName+"表不存在！");
        }
    }

    //添加一行数据，如果期望批处理，就把Put对象追加进list集合
    public static void addRow(String tableName,String rowKey, String cf,String column,String value) throws IOException {
        /*根据HBase配置文件创建HBase的连接*/
        Connection conc = ConnectionFactory.createConnection(conf);
        /*通过连接关联指定的表，实例化被操作的表*/
        Table table = conc.getTable(TableName.valueOf(tableName));
        /*通过行键实例化数据包装对象*/
        Put put = new Put(Bytes.toBytes(rowKey));
        /*定位单元格数值封装数据*/
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(column),Bytes.toBytes(value));
        /*通过表对象执行数据的追加*/
        table.put(put);

    }

    //删除一行数据
    public static void  deleteRow(String tableName,String rowKey, String cf) throws IOException {
        /*根据HBase配置文件创建HBase的连接*/
        Connection conc = ConnectionFactory.createConnection(conf);
         /*通过连接关联指定的表，实例化被操作的表*/
        Table table = conc.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);
    }

    //删除多行数据
    public static void deleteMutiRow(String tableName,String... rowKey) throws IOException {
         /*根据HBase配置文件创建HBase的连接*/
        Connection conc = ConnectionFactory.createConnection(conf);
         /*通过连接关联指定的表，实例化被操作的表*/
        Table table = conc.getTable(TableName.valueOf(tableName));
        /*封装要删除的记录集合*/
        List<Delete> list = new ArrayList<Delete>();
        for(String key:rowKey){
            /*按行键将每一条需要删除的记录进行封装*/
            Delete delete = new Delete(Bytes.toBytes(key));
            /*将标记删除对象添加进集合*/
            list.add(delete);
        }
        /*table对象执行删除操作*/
        table.delete(list);
    }

    //扫描数据
    public static void getAllRows(String tableName) throws IOException {
         /*根据HBase配置文件创建HBase的连接*/
        Connection conc = ConnectionFactory.createConnection(conf);
         /*通过连接关联指定的表，实例化被操作的表*/
        Table table = conc.getTable(TableName.valueOf(tableName));
        /*指定如何去扫描数据--即扫描规则----这里默认是全表扫描，也可以指定startRow规则*/
        Scan scan =new Scan();
        //扫描最新版本的数据
        //scan.setMaxVersions();
        //table用指定的扫描规则扫描数据，得到一个结果集迭代器
        ResultScanner scanner = table.getScanner(scan);
        /*forEach遍历迭代器，不能改变迭代器里面的数据结构；for i 就可以；
        * result封装了每一条行键记录的所有信息，可以通过result得到这条记录的行键*/
        for(Result result:scanner){
            System.out.println("正在遍历"+Bytes.toString(result.getRow())+"这条行键的所有单元格信息！");
            /*以下输出结果为：当前记录的完整信息keyvalues={1005/info1:name/1562536919457/Put/vlen=4/seqid=0}*/
            System.out.println("当前记录的完整信息"+result);
             /*得到这条记录每一个单元格里面的信息*/
            Cell [] cells = result.rawCells();
            /*每一个单元格里面包装了这个值的所有相关信息*/
            for(Cell cell:cells){
                System.out.println("行键"+Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.println("列族"+Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("列字段"+Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("单元格值"+Bytes.toString(CellUtil.cloneValue(cell)));
            }
            System.out.println("当前行键信息遍历结束！");
        }
    }

    //根据行键得到具体的数据
    public static void getRow(String tableName,String rowKey,String cf, String column) throws IOException {
          /*根据HBase配置文件创建HBase的连接*/
        Connection conc = ConnectionFactory.createConnection(conf);
         /*通过连接关联指定的表，实例化被操作的表*/
        Table table = conc.getTable(TableName.valueOf(tableName));

        Get get = new Get(Bytes.toBytes(rowKey));
        get.addFamily(Bytes.toBytes(cf));
        get.addColumn(Bytes.toBytes(cf),Bytes.toBytes(column));
        Result result = table.get(get);
        System.out.println(result);
    }


    public static void main(String[] Args) throws IOException {
         // System.out.println(isExist("student"));
          //createTable("starff","info1","info2");
        //deleteTable("student");
//        addRow("starff","1002","info1","name1","nick1");
//        addRow("starff","1003","info1","name","tom");
//        addRow("starff","1004","info2","name","lisa");
//        addRow("starff","1005","info1","name","jack");
        //deleteRow("starff","1001","info1");
        //deleteMutiRow("starff","1002","1003","1004");
        //getAllRows("starff");
        getRow("starff","1002","info1","name1");
    }
}
