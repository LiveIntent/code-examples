# Missing Data Task

For technical reasons, the daily processing of one of our datasets has been split into 17 parts (0-f + s). The size of the dataset is expected to grow over time. We keep the latest iterations of the dataset around (assume we keep the last 7 days worth of data around), as well as the data for the first day of the month for the last 12 months.

One day a developer notices that the total amount of data appears to have decreased over the course of 6 months. Furthermore, that developer has found a specific date (20210224) where all 17 spark applications seemingly finished without error, but some output files appear to be missing. On all other days that we still have data from, all files appear to be present.

- What short term steps can we take to ensure that we don't have an on-going data leak?
- How would you approach the problem of figuring out what is going on (i.e. coming up with a long term solution)?

the 17 jobs write to separate locations, for instance job 0 will write to 

```text
/daily-unified-mapping-aggregation-updated-prior/0/<date>/
```

while job 1 will write to 

```text
/daily-unified-mapping-aggregation-updated-prior/1/<date>/
```

In [success-markers](success-markers), you can find the success markers from each of the 17 jobs for the anomalous date 20210224.

The job is a spark application compiled against spark 2.4.7 and written in scala. It is the same application that is invoked for each run, but each invocation is passed different parameters to tell the application which partition of the dataset to process. When the job is ready to write data, the following function is called


```text
  /**
   * Save output from RDD[T] to a Hadoop files using the new Hadoop API.
   *
   * If `partitioned` flag is set to false we assume we receive a simple `String` and first convert the
   * RDD[String] to a pair-rdd using `NullWritable` as the key and `Text` as the value.
   *
   * Otherwise we use the `RDD[(String, String)]` that we receive as-is.
   *
   * Additionally, as the saveAsNewAPIHadoopDataset method does not provide an argument
   * to specify compression (in contrast to the old API) we set it on the `Configuration`
   * itself.
   *
   * @param rdd RDD to save (will be converted to key-value pair)
   * @param path path to save RDD to
   * @param keyClass the key class of the final pair rdd
   * @param valueClass the value class of the final pair rdd
   * @param outputFormatClass the output format used for writing
   * @param conf the hadoop configuration
   * @param codec optional compression codec class
   */
  private def saveAsHadoopFileUsingNewApi[T](
                        rdd: RDD[T],
                        path: String,
                        partitioned: Boolean = false,
                        keyClass: Class[_] = classOf[NullWritable],
                        valueClass: Class[_] = classOf[Text],
                        outputFormatClass: Class[_ <: OutputFormat[_, _]] = classOf[TextOutputFormat[NullWritable, Text]],
                        conf: Configuration = sc.hadoopConfiguration,
                        codec: Option[Class[_ <: CompressionCodec]] = None): Unit = {

    /*
    Set output key and value classes as well as the format class.
     */
    val job = NewAPIHadoopJob.getInstance(conf)
    job.setOutputKeyClass(keyClass)
    job.setOutputValueClass(valueClass)
    job.setOutputFormatClass(outputFormatClass)
    val jobConfiguration = job.getConfiguration
    jobConfiguration.set("mapreduce.output.fileoutputformat.outputdir", path)

    /*
    If a compression codec was set we specify it as compression codec for this job. This could also be
    achieved by using [[FileOutputFormat.setOutputCompressorClass]] and [[FileOutputFormat.setCompressOutput]]
    whose calls are equivalent to the below.
    [[SequenceFileOutputFormat.setOutputCompressionType]] would be needed to set it to block compression.
    Instead we opt for a simple configuration flag setting on [[Configuration]] directly,
    as it seems less confusing than using static functions on abstract classes for setting these.
     */
    for (c <- codec) {
      jobConfiguration.set("mapreduce.output.fileoutputformat.compress", "true")
      jobConfiguration.set("mapreduce.output.fileoutputformat.compress.codec", c.getCanonicalName)
      jobConfiguration.set("mapreduce.output.fileoutputformat.compress.type",
        CompressionType.BLOCK.toString)
    }

    if(!partitioned) {
      /*
      Convert RDD to pair rdd with a null writable key.
       */
      val nullWritableClassTag = implicitly[ClassTag[NullWritable]]
      val textClassTag = implicitly[ClassTag[Text]]
      val r: RDD[(NullWritable, Text)] = rdd.mapPartitions { iter =>
        val text = new Text()
        iter.map { x =>
          text.set(x.toString)
          (NullWritable.get(), text)
        }
      }
      RDD.rddToPairRDDFunctions(r)(nullWritableClassTag, textClassTag, null).saveAsNewAPIHadoopDataset(jobConfiguration)

    } else {
      rdd.asInstanceOf[RDD[(String, String)]].saveAsNewAPIHadoopDataset(jobConfiguration)
    }
  }
```
