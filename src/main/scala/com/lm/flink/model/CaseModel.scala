package com.lm.flink.model

import java.util.Date

/**
 * @Classname CaseModel
 * @Description TODO
 * @Date 2020/9/21 15:37
 * @Created by limeng
 */
case class SourceBean(id:String,name:String,age:Int,ctime:Date) extends Serializable

case class WordWithCount(word:String,count:Long) extends Serializable
