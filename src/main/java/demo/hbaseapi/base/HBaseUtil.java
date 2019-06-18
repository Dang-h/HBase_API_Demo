package demo.hbaseapi.base;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class HBaseUtil {

    // 获取Zookeeper连接，创建一个单例
    private static Connection connection = null;

    static {
        try {
            //获取配置对象
            Configuration conf = HBaseConfiguration.create();
            // 传入Zookeeper配置
            conf.set("hbase.zookeeper.quorum", "hadoop101,hadoop102,hadoop103");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            // 传入获取Zookeeper连接的配置
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 创建表
     * 示例：create 'student','info'
     *
     * @param tableName：表名
     * @param families：列族名
     */
    public static void createTable(String tableName, String... families) throws IOException {

        // 获取Admin对象
        Admin admin = connection.getAdmin();
        HTableDescriptor desc;

        try {
            // 判断表是否存在
            if (admin.tableExists(TableName.valueOf(tableName))) {
                System.err.println("table:" + tableName + " exists");
                return;
            }

            // 2. 创建表的描述器
            desc = new HTableDescriptor(TableName.valueOf(tableName));

            // 3. 传入列族信息
            for (String family : families) {
                //5. 创建列族描述器
                HColumnDescriptor familyDesc = new HColumnDescriptor(family);

                // 4.尝试传入列族信息，需要传入列族描述器
                desc.addFamily(familyDesc);
            }

            // 1.尝试创建表,需要表的描述器
            admin.createTable(desc);
        } finally {
            // 关闭资源
            admin.close();
        }

    }

    /**
     * 删除表格
     * 示例：disable 'student'
     * drop 'student'
     *
     * @param tableName
     * @throws IOException
     */
    public static void dropTable(String tableName) throws IOException {
        Admin admin = connection.getAdmin();

        if (!admin.tableExists(TableName.valueOf(tableName))) {
            System.err.println("table: " + tableName + " not exists.");
            return;
        }

        try {
            // 2. disable表
            admin.disableTable(TableName.valueOf(tableName));

            // 1. 尝试删除表，删除之前需要disable表
            admin.deleteTable(TableName.valueOf(tableName));
        } finally {

            // 3. 关闭资源
            admin.close();
        }

    }


    /**
     * 判断表是否存在
     *
     * @param tableName
     * @return
     * @throws IOException
     */
    public static boolean tableExists(String tableName) throws IOException {
        Admin admin = connection.getAdmin();
        try {
            return admin.tableExists(TableName.valueOf(tableName));
        } finally {
            admin.close();
        }
    }


    /**
     * 插入一行中的一列数据
     * 示例：put 'student','1001','info:name','Nick'
     *
     * @param tableName
     * @param rowKey
     * @param family
     * @param column
     * @param value
     * @throws IOException
     */
    public static void putCell(String tableName, String rowKey, String family, String column, String value) throws IOException {

        if (!tableExists(tableName)) {
            System.err.println("Table：" + tableName + " not exits!");
            return;
        }

        // 1. 获取table对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        try {
            // 3. 创建put对象,为指定的行创建Put操作
            Put put = new Put(Bytes.toBytes(rowKey));

            // 4. 传入数据,将指定的列的值添加到此Put操作中。
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));

            // 2. 尝试put数据，需要传入put数据（创建put对象）
            table.put(put);
        } finally {

            // 5. 关闭资源
            table.close();
        }

    }

    /**
     * 删除某个rowKey的数据
     * 示例：deleteall 'student','1002'
     *
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    public static void deleteAll(String tableName, String rowKey) throws IOException {

        if (!tableExists(tableName)) {
            System.err.println("Table：" + tableName + " not exits!");
            return;
        }

        Table table = connection.getTable(TableName.valueOf(tableName));

        try {
            Delete delete = new Delete(Bytes.toBytes(rowKey));

            table.delete(delete);
        } finally {

            table.close();
        }

    }


    /**
     * 删除一行中的一个列族
     * 示例：delete 'student','1002','info'
     *
     * @param tableName
     * @param rowKey
     * @param family
     * @throws IOException
     */
    public static void deleteFamily(String tableName, String rowKey, String family) throws IOException {
        if (!tableExists(tableName)) {
            System.err.println("Table：" + tableName + " not exits!");
            return;
        }

        Table table = connection.getTable(TableName.valueOf(tableName));
        try {
            Delete delete = new Delete(Bytes.toBytes(rowKey));

            delete.addFamily(Bytes.toBytes(family));

            table.delete(delete);
        } finally {
            table.close();
        }

    }


    /**
     * 删除一行中的一个列的最新版本
     *
     * @param tableName
     * @param rowKey
     * @param family
     * @param column
     * @throws IOException
     */
    public static void deleteCell(String tableName, String rowKey, String family, String column) throws IOException {
        if (!tableExists(tableName)) {
            System.err.println("Table：" + tableName + " not exits!");
            return;
        }

        Table table = connection.getTable(TableName.valueOf(tableName));

        try {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            delete.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));

            table.delete(delete);
        } finally {
            table.close();
        }
    }

    /**
     * 获取一行的数据
     * 示例：get 'student','1001'
     *
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    public static void getRow(String tableName, String rowKey) throws IOException {
        if (!tableExists(tableName)) {
            System.err.println("Table：" + tableName + " not exits!");
            return;
        }

        Table table = connection.getTable(TableName.valueOf(tableName));

        try {
            // 2. 创建Get对象，指定rowKey
            Get get = new Get(Bytes.toBytes(rowKey));

            // 1. 尝试get，需要传入一个Get对象；获得返回值
            Result result = table.get(get);

            // 3. 获取列数据，一个cell一个列
            Cell[] cells = result.rawCells();

            for (Cell cell : cells) {
                //获取列名
                byte[] columnByte = CellUtil.cloneQualifier(cell);
                String columnStr = Bytes.toString(columnByte);

                // 获取一列中的数据
                byte[] valueByte = CellUtil.cloneValue(cell);
                String valueStr = Bytes.toString(valueByte);

                // 输出一列数据
                System.out.println(columnStr + ":" + valueStr);
            }
        } finally {

            table.close();
        }

    }

    public static void getRowByColumns(String tableName, String family, Map<String, String> map) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));

        // 2. 创建scan对象；跨所有行创建扫描操作
        Scan scan = new Scan();
        ResultScanner resultScanner = null;

        try {
            // 3. 指定过滤条件
            SingleColumnValueFilter filter1 = new SingleColumnValueFilter(Bytes.toBytes(family),
                    Bytes.toBytes(map.keySet().toArray()[0].toString()),
                    CompareFilter.CompareOp.EQUAL,
                    Bytes.toBytes(map.get(map.keySet().toArray()[0].toString())));
            // 当数据中有不匹配过滤规则的数据，过滤
            filter1.setFilterIfMissing(true);

            SingleColumnValueFilter filter2 = new SingleColumnValueFilter(Bytes.toBytes(family),
                    Bytes.toBytes(map.keySet().toArray()[1].toString()),
                    CompareFilter.CompareOp.EQUAL,
                    Bytes.toBytes(map.get(map.keySet().toArray()[1].toString())));
            filter2.setFilterIfMissing(true);

            // 4. 指定过滤条件连接规则
            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);

            // 5. 导入过滤规则
            filterList.addFilter(filter1);
            filterList.addFilter(filter2);

            // 6. 传入过滤器
            scan.setFilter(filterList);

            // 1. 获取一行数据的集合，数据一批批返回，scanner需要建立连接
            // 需要传入scan对象
            resultScanner = table.getScanner(scan);

            // 7. 获取一行中cell的数据
            for (Result result : resultScanner) {
                //获取cell中的数据
                for (Cell cell : result.rawCells()) {
                    String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
                    String columnStr = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String valueStr = Bytes.toString(CellUtil.cloneValue(cell));

                    System.out.println(rowKey + "-" + columnStr + ":" + valueStr);
                }
            }
        } finally {

            // 8. 关闭资源
            resultScanner.close();
            table.close();
        }

    }


    public static void main(String[] args) throws IOException {
//        createTable("test","test_info");
//        dropTable("test");
//        putCell("student","1003", "info", "name", "Tom");
//        putCell("student","1003", "info", "name", "Jerry");
//        deleteAll("student", "1001");
//            deleteFamily("employee", "1002", "info");
//        deleteCell("student","1003", "info", "name");
//        getRow("employee", "1001");

        HashMap<String, String> map = new HashMap<>();

        map.put("age", "18");
        map.put("gender", "girl");

        getRowByColumns("student","info", map);
    }
}
