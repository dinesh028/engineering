package com.aquaifer.producer

import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.PartitionInfo
import scala.collection.Map
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import org.apache.kafka.clients.producer.RoundRobinPartitioner
import org.apache.kafka.common.utils.Utils
import com.google.gson.JsonParser
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.Properties
import java.io.FileInputStream

/**
 * Partition Records based on a primary key present in Value Message
 */
class KeyPartitioner extends RoundRobinPartitioner {

  /**
   * Logger
   */
  private val LOG: Logger = LoggerFactory.getLogger(getClass)

  var configMap: Map[String, String] = Map[String, String]()

/*
Get HashCode of Value of PrimaryKey in JSON Message
Get Reminder of Division opertion (Mod) = Positive Hash Code / Number of Partitions  
*/
  @Override
  override def partition(topic: String, key: Object, keyBytes: Array[Byte], value: Object, valueBytes: Array[Byte], cluster: Cluster): Int = {
    val keyStr = new String(keyBytes)
    val valueStr = new String(valueBytes)
    try {

      val mapKey = (keyStr).toUpperCase()
      //LOG.info(mapKey)
      if (configMap.contains(mapKey)) {
        //LOG.info("contains "+mapKey)
        val partitions: java.util.List[PartitionInfo] = cluster.partitionsForTopic(topic)
        val numPartitions: Int = partitions.size()
        val hashCode = getHashValue(valueStr, configMap.get(mapKey).get)
        val availablePartitions: java.util.List[PartitionInfo] = cluster.availablePartitionsForTopic(topic)
        if (!availablePartitions.isEmpty()) {
          val part: Int = Utils.toPositive(hashCode) % availablePartitions.size();
          return availablePartitions.get(part).partition();
        } else {
          // no partitions are available, give a non-available partition
          return Utils.toPositive(hashCode) % numPartitions;
        }
      } else {
        // Else use Round Robin
        return super.partition(topic, key, keyBytes, value, valueBytes, cluster)
      }

    } catch {
      case t: Exception => {
        LOG.error("FAILED: To Partition", t)
        // In case of exception go with RoundRobin
        super.partition(topic, key, keyBytes, value, valueBytes, cluster)
      }
    }
  }

/*
Parse JSON Message to get value of PrimaryKey, and return hash code of this value.
*/
  def getHashValue(value: String, keyName: String): Int = {
    val jsonObject = new JsonParser().parse(value).getAsJsonObject()
    val hashValue = jsonObject.get(keyName).getAsString.hashCode()

    LOG.debug("Partitioning on hash [{}]", hashValue)
    hashValue
  }

  @Override
  override def close(): Unit = {
    // Do Nothing
  }

/*
Reads a property FILE which consist of mapping like TOPIC_STREAMING.TOPIC1.FILTER.Key=PrimaryKey. 
This is parsed and set as configMap like Key=PrimaryKey
*/
  @Override
  override def configure(configs: java.util.Map[String, _]): Unit = {
    val prefix = (TOPIC_STREAMING).toUpperCase()
    val contains = (FILTER).toUpperCase()
    // val filteredEntitiesMap = map.filterKeys(x => x.toUpperCase().startsWith(prefix)).map(f => (f._1.toUpperCase().replace(prefix, ""), f._2))
    // configMap = configs.asScala
    //  .filterKeys(x => x.toUpperCase().startsWith(prefix) && x.toUpperCase().contains(FILTER))
    // .map(f => (f._1.toUpperCase().replace(prefix, "").replace(FILTER, ""), f._2.asInstanceOf[String]))

    val fileName = System.getProperty(JOB_PROPS_FILE_NAME)

    LOG.info("Reading properties file [{}]", fileName)

    val props = new Properties()
    props.load(new FileInputStream(fileName))
    val map = props.asScala
    // LOG.info("Map [{}]", map)
    configMap = map.filterKeys(x => x.toUpperCase().startsWith(prefix) && x.toUpperCase().contains(contains))
      .map(f => (f._1.toUpperCase().replaceAll(prefix+"[^&]*"+contains, ""), f._2.asInstanceOf[String]))
    LOG.info("Map [{}]", configMap)
  }

}