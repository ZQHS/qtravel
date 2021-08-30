package com.qianfeng.bigdata.realtime.flink.util.es

import java.net.{InetAddress, InetSocketAddress}

import com.qianfeng.bigdata.realtime.flink.constant.QRealTimeConstant
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.http.HttpHost

/**
 * es的config配置文件加载，，返回config对象
 */
object ESConfigUtil {

  //定义两个变量
  var esConfigSocket: ESConfigSocket = null
  var esConfigHttpHost : ESConfigHttpHost = null
  //定义封装socket的配置类
  class ESConfigSocket(var config: java.util.HashMap[String, String], var transportAddresses: java.util.ArrayList[InetSocketAddress])
  //定义封装http的配置类
  class ESConfigHttpHost(var config: java.util.HashMap[String, String], var transportAddresses: java.util.ArrayList[HttpHost])

  /**
   * 客户端连接
   * @param configPath
   * @return
   */
  def getConfigSocket(configPath: String): ESConfigSocket = {
    //使用类加载器加载配置文件
    val configStream = this.getClass.getClassLoader.getResourceAsStream(configPath)
    if (null == esConfigSocket) {
      val mapper = new ObjectMapper()
      val configJsonObject = mapper.readTree(configStream)

      val configJsonNode = configJsonObject.get("config")

      val config = {
        val configJsonMap = new java.util.HashMap[String, String]
        val it = configJsonNode.fieldNames()
        while (it.hasNext) {
          val key = it.next()
          configJsonMap.put(key, configJsonNode.get(key).asText())
        }
        configJsonMap
      }

      val addressJsonNode = configJsonObject.get("address")
      val addressJsonArray = classOf[ArrayNode].cast(addressJsonNode)
      val transportAddresses = {
        val transportAddresses = new java.util.ArrayList[InetSocketAddress]
        val it = addressJsonArray.iterator()
        while (it.hasNext) {
          val detailJsonNode: JsonNode = it.next()
          val ip = detailJsonNode.get("ip").asText()
          val port = detailJsonNode.get("port").asInt()
          transportAddresses.add(new InetSocketAddress(InetAddress.getByName(ip), port))
        }
        transportAddresses
      }

      esConfigSocket = new ESConfigSocket(config, transportAddresses)
    }
    esConfigSocket
  }


  /**
   *
   * @param configPath
   * @return
   */
  def getConfigHttpHost(configPath: String): ESConfigHttpHost = {
    val configStream = this.getClass.getClassLoader.getResourceAsStream(configPath)
    if (null == esConfigHttpHost) {
      val mapper = new ObjectMapper()
      val configJsonObject = mapper.readTree(configStream)

      val configJsonNode = configJsonObject.get("config")

      val config = {
        val configJsonMap = new java.util.HashMap[String, String]
        val it = configJsonNode.fieldNames()
        while (it.hasNext) {
          val key = it.next()
          configJsonMap.put(key, configJsonNode.get(key).asText())
        }
        configJsonMap
      }

      val addressJsonNode = configJsonObject.get("address")
      val addressJsonArray = classOf[ArrayNode].cast(addressJsonNode)
      val transportAddresses = {
        val httpHosts = new java.util.ArrayList[HttpHost]
        val it = addressJsonArray.iterator()
        while (it.hasNext) {
          val detailJsonNode: JsonNode = it.next()
          val ip = detailJsonNode.get("ip").asText()
          val port = detailJsonNode.get("port").asInt()
          val schema = "http"

          val httpHost = new HttpHost(ip, port, schema)
          httpHosts.add(httpHost)
        }
        httpHosts
      }

      esConfigHttpHost = new ESConfigHttpHost(config, transportAddresses)
    }
    esConfigHttpHost
  }

  //测试
  def main(args: Array[String]): Unit = {
    println(getConfigSocket(QRealTimeConstant.ES_CONFIG_PATH).config.get("cluster.name"))

    println(getConfigHttpHost(QRealTimeConstant.ES_CONFIG_PATH).transportAddresses.get(0).getHostName)
  }
}
