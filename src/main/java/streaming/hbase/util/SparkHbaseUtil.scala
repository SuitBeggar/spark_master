package streaming.hbase.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ArrayBuffer

/**
  * Created by fangyitao on 2019/11/20.
  */
object SparkHbaseUtil {

  private val config: Configuration = HBaseConfiguration.create()   // ��װhbase�Ĳ���

  private val conn: Connection = ConnectionFactory.createConnection(config)   // ����hbase�����Ӳ���

  private val admin: Admin = conn.getAdmin    // ��ȡhbase�Ŀͻ��˲���


  /**
    * ������
    * @param tableNameStr
    * @param columnFamily
    * @return
    */
  def createTable(tableNameStr : String,columnFamily : String): Table ={
      val tableName : TableName = TableName.valueOf(tableNameStr)

      // �������������
      val hTableDescriptor = new HTableDescriptor(tableName)

      // ���������������
      val hColumnDescriptor = new HColumnDescriptor(columnFamily)

      hTableDescriptor.addFamily(hColumnDescriptor)

      // ����������򴴽���
      if (!admin.tableExists(tableName)) {
        admin.createTable(hTableDescriptor)
      }

      conn.getTable(tableName)

  }

  /**
    * ����rowkey,������ѯ����
    * @param rowkey rowkey
    * @param columnFamily ������
    * @param column ����
    * @return ����
    */
  def getData(tableNameStr: String, rowkey: String, columnFamily: String, column: String): String = {

    val table: Table = createTable(tableNameStr, columnFamily)

    var tmp = ""

    try {
      val bytesRowkey = Bytes.toBytes(rowkey)
      val get: Get = new Get(bytesRowkey)
      val result: Result = table.get(get)
      val values: Array[Byte] = result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(column.toString))

      if (values != null && values.size > 0) {
        tmp = Bytes.toString(values)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      table.close()
    }
    tmp
  }

  /**
    * ������ȡ�е�����
    * @param tableNameStr ����
    * @param rowkey rowkey
    * @param columnFamily ����
    * @param columnList �е������б�
    * @return
    */
  def getData(tableNameStr: String, rowkey: String, columnFamily: String, columnList:List[String]) = {
    val table: Table = createTable(tableNameStr, columnFamily)
    var tmp = ""

    try {
      val bytesRowkey = Bytes.toBytes(rowkey)
      val get: Get = new Get(bytesRowkey)
      val result: Result = table.get(get)
      val valueMap = collection.mutable.Map[String, String]()

      //columnList.map()
//      columnList.map(col =>  val values: Array[Byte] = result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(col))
//
//          if (values != null && values.size > 0) {
//            col -> Bytes.toString(values)
//          }
//          else {
//            ""->""
//          }
//      ).filter(_._1 != "").toMap
    } catch {
      case e: Exception =>
        e.printStackTrace()
        Map[String, String]()
    } finally {
      table.close()
    }
  }

  /**
    * ����/����һ������
    * @param rowKey rowkey
    * @param columnFamily ����
    * @param column ����
    * @param data ����
    */
  def putData(tableNameStr: String, rowKey: String, columnFamily: String, column: String, data: String) = {

    val table: Table = init(tableNameStr, columnFamily)

    try {
      val put: Put = new Put(Bytes.toBytes(rowKey))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column.toString), Bytes.toBytes(data.toString))
      table.put(put)
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      table.close()
    }
  }

  /**
    * ʹ��Map��װ���ݣ�����/����һ������
    * @param rowKey rowkey
    * @param columnFamily ����
    * @param mapData key:������value����ֵ
    */
  def putMapData(tableNameStr: String, rowKey: String, columnFamily: String, mapData: Map[String, String]) = {
    val table: Table = init(tableNameStr, columnFamily)
    try {
      val put: Put = new Put(Bytes.toBytes(rowKey))
      if (mapData.size > 0) {
        for ((k, v) <- mapData) {
          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(k), Bytes.toBytes(v))
        }
      }
      table.put(put)
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      table.close()
    }
  }

  /**
    * ����rowkeyɾ��һ������
    *
    * @param tableNameStr ����
    * @param rowkey rowkey
    */
  def deleteData(tableNameStr:String, rowkey:String, columnFamily:String) = {
    val tableName: TableName = TableName.valueOf(tableNameStr)

    // �������������
    val hTableDescriptor = new HTableDescriptor(tableName)
    // ���������������
    val hColumnDescriptor = new HColumnDescriptor(columnFamily)

    hTableDescriptor.addFamily(hColumnDescriptor)
    // ����������򴴽���
    if (!admin.tableExists(tableName)) {
      admin.createTable(hTableDescriptor)
    }

    val table = conn.getTable(tableName)

    try {
      val delete = new Delete(Bytes.toBytes(rowkey))
      table.delete(delete)
    }
    catch {
      case e:Exception => e.printStackTrace()
    }
    finally {
      table.close()
    }
  }

}
