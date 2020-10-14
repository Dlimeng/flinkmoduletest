package com.lm.flink.datastream.source

import net.sf.json.JSONObject
import org.apache.flink.api.common.serialization.{DeserializationSchema, SerializationSchema}
import org.apache.flink.api.common.typeinfo.TypeInformation
/**
 * @Classname KafkaEventSchema
 * @Description TODO
 * @Date 2020/10/14 20:31
 * @Created by limeng
 *
 * 自定义Json反序列化
 */
class KafkaEventSchema extends DeserializationSchema[JSONObject] with SerializationSchema[JSONObject] {
  override def deserialize(message: Array[Byte]): JSONObject = {
    JSONObject.fromObject(new String(message))
  }

  override def isEndOfStream(nextElement: JSONObject): Boolean = false

  override def serialize(element: JSONObject): Array[Byte] = {
    element.toString.getBytes
  }

  override def getProducedType: TypeInformation[JSONObject] = {
    TypeInformation.of(classOf[JSONObject])
  }
}
