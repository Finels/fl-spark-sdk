
[![License](https://img.shields.io/github/license/Finels/fl-spark-sdk.svg)](https://github.com/Finels/fl-spark-sdk)&nbsp;

**基于Apache Spark的开发工具包，帮助数据中台实现ETL开发标准化，同时可基于此SDK实现诸多数据中台的基础能力，包括：流批一体，自定义数据血缘，数据源与数据汇统一管理等。**


# 目录

* [特性](#特性)
* [如何使用](#如何使用)


# 特性

- 目前提供了多种数据源接入：Hbase，Mongo，Kafka，JDBC类型（mysql，sqlserver，pgsql）  
- Hbase的读写默认实现了二级索引和rowkey加盐，默认按照20个分区进行加盐，如需修改，请查看`org.fasteam.hbase.entry.effkey.ConsistentHashing `  
- 二级索引目前提供了基于elasticsearch的实现，也可以使用其他组件（如redis），需自行实现接口
- Hbase读写提供了日志记录的功能，用于实现数据血缘，可以在配置文件中关闭
- 提供了structured streaming的kafka，hbase等读写器，封装程度不高，可以直接使用。可参考`fl-spark-sdk-it`中的例子
- 接入的连接信息提供了`INTERNAL`和`EXTERNAL`两种方式进行管理，`INTERNAL`用于本地调试，读取`fl-spark-sdk-core`下的`default-rules.xml`文件  


# 如何使用  

1. clone项目，根据实际情况修改版本，目前Spark版本最高支持3.1.x, Hbase版本最高支持2.3.x,JDBC连接类型的数据库（MYSQL,SQLSERVER,PGSQL）版本可任意修改，根据实际情况为准

2. 执行 `mvn install` 安装到本地仓库  

3. 新建一个空项目，直接引用`fl-spark-sdk-starter`模块，已集成所有能力  
```
<dependency>
       <groupId>org.fasteam.sdk</groupId>
       <artifactId>fl-spark-sdk-starter</artifactId>
       <version>${spark.sdk.version}</version>
       <scope>provided</scope>
</dependency>

```

4. 写具体的业务逻辑，主类需继承 `org.fasteam.sdk.core.SparkUserDefineApplication` 其抽象方法是入口，方法内可以调用SDK中所封装的能力。

   各封装的能力类请参考`abilities`模块中的各功能模块，也可参考`fl-spark-sdk-it`模块中的测试示例

5. 运行项目: 所有的Spark项目的执行入口主类都是 `org.fasteam.sdk.core.SparkProcessor`，需要传入经过Base64转码的命令行参数（主要用于防止参数传递时出现各种格式问题），参数格式统一为JSON  

   完整的参数格式如下：  
```
{   
    "runClass":"org.fasteam.sdk.it.hbase.UnitReadTest",  //必传，业务逻辑主类，即继承SparkUserDefineApplication的子类
    "appName":"HbaseReadTest",                           //必传，应用名称，即sparkContext的AppName
    "master":"local[*]",                                 //跟spark-submit的master参数一样，可为空，为空时默认为yarn
    "logLevel":"warn",                                   //可为空，为空时日志级别默认为info
    "confLocate":"remote",                               //值必须为'local'或者'remote'，为'remote'时'confUri'项必填
    "confUri":"http://127.0.0.1/nacos/v1/cs/configs?dataId=fl-sprk-sdk-rules.xml&group=DEFAULT_GROUP"  
                                                         //指向外部配置文件的url，可以是nacos或者其他静态站点，但返回格式必须和'default-rules.xml'一样
}

```   
可以参考`fl-spark-sdk-it`模块中的测试示例
