# **Disaggregated HDP Spark and Hive with MinIO**

## **1. Cloud-native Architecture**

![cloud-native](https://github.com/minio/minio/blob/master/docs/bigdata/images/image1.png?raw=true "cloud native architecture")

Kubernetes manages stateless Spark and Hive containers elastically on the compute nodes. Spark has native scheduler integration with Kubernetes. Hive, for legacy reasons, uses YARN scheduler on top of Kubernetes. 

All access to MinIO object storage is via S3/SQL SELECT API. In addition to the compute nodes, MinIO containers are also managed by Kubernetes as stateful containers with local storage (JBOD/JBOF) mapped as persistent local volumes. This architecture enables multi-tenant MinIO, allowing isolation of data between customers.

MinIO also supports multi-cluster, multi-site federation similar to AWS regions and tiers. Using MinIO Information Lifecycle Management (ILM), you can configure data to be tiered between NVMe based hot storage, and HDD based warm storage. All data is encrypted with per-object key. Access Control and Identity Management between the tenants are managed by MinIO using OpenID Connect or Kerberos/LDAP/AD.

## **2. Prerequisites**

*  Install Hortonworks Distribution using this [guide.](https://docs.hortonworks.com/HDPDocuments/Ambari-2.7.1.0/bk_ambari-installation/content/ch_Installing_Ambari.html)
   *   [Setup Ambari](https://docs.hortonworks.com/HDPDocuments/Ambari-2.7.1.0/bk_ambari-installation/content/set_up_the_ambari_server.html) which automatically sets up YARN
   *   [Installing Spark](https://docs.hortonworks.com/HDPDocuments/HDP3/HDP-3.0.1/installing-spark/content/installing_spark.html)
*  Install MinIO Distributed Server using one of the guides below.
   *   [Deployment based on Kubernetes](https://docs.min.io/docs/deploy-minio-on-kubernetes.html#minio-distributed-server-deployment)
   *   [Deployment based on MinIO Helm Chart](https://github.com/helm/charts/tree/master/stable/minio)

## **3. Configure Hadoop, Spark, Hive to use MinIO**

After successful installation navigate to the Ambari UI `http://<ambari-server>:8080/` and login using the default credentials: [**_username: admin, password: admin_**]

![ambari-login](https://github.com/minio/minio/blob/master/docs/bigdata/images/image3.png?raw=true "ambari login")

### **3.1 Configure Hadoop**

Navigate to **Services** -> **HDFS** -> **CONFIGS** -> **ADVANCED** as shown below

![hdfs-configs](https://github.com/minio/minio/blob/master/docs/bigdata/images/image2.png?raw=true "hdfs advanced configs")

Navigate to **Custom core-site** to configure MinIO parameters for `_s3a_` connector

![s3a-config](https://github.com/minio/minio/blob/master/docs/bigdata/images/image5.png?raw=true "custom core-site")

Add the following optimal entries for _core-site.xml_ to configure _s3a_ with **MinIO**. Most important options here are

*  _fs.s3a.access.key=minio_ (Access Key to access MinIO instance, this is obtained after the deployment on k8s)
*  _fs.s3a.secret.key=minio123_ (Secret Key to access MinIO instance, this is obtained after the deployment on k8s)
*  _fs.s3a.endpoint=`http://minio-address/`_
*  _fs.s3a.path.style.acces=true_

![s3a-config](https://github.com/minio/minio/blob/master/docs/bigdata/images/image4.png?raw=true "custom core-site s3a")

The rest of the other optimization options are discussed in the links below

*  [https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html)
*  [https://hadoop.apache.org/docs/r3.1.1/hadoop-aws/tools/hadoop-aws/committers.html](https://hadoop.apache.org/docs/r3.1.1/hadoop-aws/tools/hadoop-aws/committers.html)

Once the config changes are applied, proceed to restart **Hadoop** services.

![hdfs-services](https://github.com/minio/minio/blob/master/docs/bigdata/images/image7.png?raw=true "hdfs restart services")

### **3.2 Configure Spark2**

Navigate to **Services** -> **Spark2** -> **CONFIGS** as shown below

![spark-config](https://github.com/minio/minio/blob/master/docs/bigdata/images/image6.png?raw=true "spark config")

Navigate to “**Custom spark-defaults**” to configure MinIO parameters for `_s3a_` connector

![spark-config](https://github.com/minio/minio/blob/master/docs/bigdata/images/image9.png?raw=true "spark defaults")

Add the following optimal entries for _spark-defaults.conf_ to configure Spark with **MinIO**.

*  _spark.hadoop.fs.s3a.committer.magic.enabled=true_
*  _spark.hadoop.fs.s3a.committer.name=magic_
*  _spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem_
*  _spark.hadoop.fs.s3a.path.style.access=true_
*  _spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a=org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory_

![spark-config](https://github.com/minio/minio/blob/master/docs/bigdata/images/image8.png?raw=true "spark custom configuration")

Once the config changes are applied, proceed to restart **Spark** services.

![spark-config](https://github.com/minio/minio/blob/master/docs/bigdata/images/image12.png?raw=true "spark restart services")

### **3.3 Configure Hive**

Navigate to **Services** -> **Hive** -> **CONFIGS**-> **ADVANCED** as shown below

![hive-config](https://github.com/minio/minio/blob/master/docs/bigdata/images/image10.png?raw=true "hive advanced config")

Navigate to “**Custom hive-site**” to configure MinIO parameters for `_s3a_` connector

![hive-config](https://github.com/minio/minio/blob/master/docs/bigdata/images/image11.png?raw=true "hive advanced config")

Add the following optimal entries for `hive-site.xml` to configure Hive with **MinIO**.

*  _hive.blobstore.use.blobstore.as.scratchdir=true_
*  _hive.exec.input.listing.max.threads=50_
*  _hive.load.dynamic.partitions.thread=25_
*  _hive.metastore.fshandler.threads=50_
*  _hive.mv.files.threads=40_
*  _mapreduce.input.fileinputformat.list-status.num-threads=50_

For more information about these options please visit [https://www.cloudera.com/documentation/enterprise/5-11-x/topics/admin_hive_on_s3_tuning.html](https://www.cloudera.com/documentation/enterprise/5-11-x/topics/admin_hive_on_s3_tuning.html)

![hive-config](https://github.com/minio/minio/blob/master/docs/bigdata/images/image13.png?raw=true "hive advanced custom config")

Once the config changes are applied, proceed to restart all Hive services.

![hive-config](https://github.com/minio/minio/blob/master/docs/bigdata/images/image14.png?raw=true "restart hive services")

## **4. Run Sample Applications**

After installing Hive, Hadoop and Spark successfully, we can now proceed to run some sample applications to see if they are configured appropriately.  We can use Spark Pi and Spark WordCount programs to validate our Spark installation. We can also explore how to run Spark jobs from the command line and Spark shell.

### **4.1 Spark Pi**

Test the Spark installation by running the following compute intensive example, which calculates pi by “throwing darts” at a circle. The program generates points in the unit square ((0,0) to (1,1)) and counts how many points fall within the unit circle within the square. The result approximates pi.

Follow these steps to run the Spark Pi example:

*  Login as user **‘spark’**.
*  When the job runs, the library can now use **MinIO** during intermediate processing.
*  Navigate to a node with the Spark client and access the spark2-client directory:

```
cd /usr/hdp/current/spark2-client
su spark
```

*  Run the Apache Spark Pi job in yarn-client mode, using code from **org.apache.spark**:

```
./bin/spark-submit --class org.apache.spark.examples.SparkPi \
    --master yarn-client \
    --num-executors 1 \
    --driver-memory 512m \
    --executor-memory 512m \
    --executor-cores 1 \
    examples/jars/spark-examples*.jar 10
```

The job should produce an output as shown below. Note the value of pi in the output.

```
17/03/22 23:21:10 INFO DAGScheduler: Job 0 finished: reduce at SparkPi.scala:38, took 1.302805 s
Pi is roughly 3.1445191445191445
```

Job status can also be viewed in a browser by navigating to the YARN ResourceManager Web UI and clicking on job history server information.

### **4.2 WordCount**

WordCount is a simple program that counts how often a word occurs in a text file. The code builds a dataset of (String, Int) pairs called counts, and saves the dataset to a file.

The following example submits WordCount code to the Scala shell. Select an input file for the Spark WordCount example. We can use any text file as input.

*  Login as user **‘spark’**.
*  When the job runs, the library can now use **MinIO** during intermediate processing.
*  Navigate to a node with Spark client and access the spark2-client directory:

```
cd /usr/hdp/current/spark2-client
su spark
```

The following example uses _log4j.properties_ as the input file:

#### **4.2.1 Upload the input file to HDFS:**

```
hadoop fs -copyFromLocal /etc/hadoop/conf/log4j.properties
          s3a://testbucket/testdata
```

#### **4.2.2  Run the Spark shell:**

```
./bin/spark-shell --master yarn-client --driver-memory 512m --executor-memory 512m
```

The command should produce an output as shown below. (with additional status messages):

```
Spark context Web UI available at http://172.26.236.247:4041
Spark context available as 'sc' (master = yarn, app id = application_1490217230866_0002).
Spark session available as 'spark'.
Welcome to


      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 2.1.0.2.6.0.0-598
      /_/

Using Scala version 2.11.8 (Java HotSpot(TM) 64-Bit Server VM, Java 1.8.0_112)
Type in expressions to have them evaluated.
Type :help for more information.

scala>
```

*  At the _scala>_ prompt, submit the job by typing the following commands, Replace node names, file name, and file location with your values:

```
scala> val file = sc.textFile("s3a://testbucket/testdata")
file: org.apache.spark.rdd.RDD[String] = s3a://testbucket/testdata MapPartitionsRDD[1] at textFile at <console>:24

scala> val counts = file.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
counts: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[4] at reduceByKey at <console>:25

scala> counts.saveAsTextFile("s3a://testbucket/wordcount")
```

Use one of the following approaches to view job output:

View output in the Scala shell:

```
scala> counts.count()
364
```

To view the output from MinIO exit the Scala shell. View WordCount job status:

```
hadoop fs -ls s3a://testbucket/wordcount
```

The output should be similar to the following:

```
Found 3 items
-rw-rw-rw-   1 spark spark          0 2019-05-04 01:36 s3a://testbucket/wordcount/_SUCCESS
-rw-rw-rw-   1 spark spark       4956 2019-05-04 01:36 s3a://testbucket/wordcount/part-00000
-rw-rw-rw-   1 spark spark       5616 2019-05-04 01:36 s3a://testbucket/wordcount/part-00001
```
