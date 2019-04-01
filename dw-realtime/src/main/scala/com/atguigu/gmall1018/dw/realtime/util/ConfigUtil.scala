package com.atguigu.gmall1018.dw.realtime.util

import java.util
import java.util.ResourceBundle

import com.alibaba.fastjson.{JSON, JSONObject}

/**
  * 配置工具类
  */
object ConfigUtil {

    // i18n == 国际化
    private val rb = ResourceBundle.getBundle("config")

    def getValueByKeyFromConfig(config : String, key : String): String = {
        ResourceBundle.getBundle(config).getString(key)
    }

    /**
      * 根据key获取配置文件中的value
      * @param key
      * @return
      */
    def getValueByKey( key : String ): String = {

        //Thread.currentThread().getContextClassLoader.getResourceAsStream("config.properties")
        rb.getString(key)
    }

    def getCondValue(attr : String): String = {
        val conditionVal = ResourceBundle.getBundle("condition").getString("condition.params.json")
        // FastJSON
        val jsonObject: JSONObject = JSON.parseObject(conditionVal)
        jsonObject.getString(attr)
    }

    def main(args: Array[String]): Unit = {
        println(getCondValue("startDate"))
    }
}
