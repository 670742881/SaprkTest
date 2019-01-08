package com.spark.demo

import java.net.URLDecoder

/**
 * Hello world!
 *
 */
object App extends Object{
  def main(args: Array[String]): Unit = {
val a=" &url=http%3A%2F%2Fm.cctv4g.com%2Fcntv%2Fresource%2Fcltv2%2FliveDetailPage.jsp%3FcontId%3D5500073800%26dataType%3D4%26stats_menuId%3D845%26stats_areaId%3D1708%26stats_areaType%3D2%26stats_contId%3D1880842%26stats_srcContType%3D4%26stats_srcContId%3D5500073800%26wdChannelName%3Dvivo%26wdVersionName%3D2.7.0%26wdClientType%3D1%26wdAppId%3D3%26wdNetType%3DWIFI%26uuid%3D06d1ad22-cf6d-309d-9d8a-6a4919c012f2&srcContType=4&appName=CCTV%E6%89%8B%E6%9C%BA%E7%94%B5%E8%A7%86++%EF%BC%88V2%EF%BC%89&netType=WIFI&areaName=%E6%AD%A3%E5%9C%A8%E7%9B%B4%E6%92%AD&contName=CCTV-13%E6%96%B0%E9%97%BB%E9%A2%91%E9%81%93&sessionId=7625A4C76B98086A7F0ACD00AA375B9F&ua=yichengtianxia&en=e_pv&uuid=06d1ad22-cf6d-309d-9d8a-6a4919c012f2&clientIp=223.90.152.7&menuName=%E7%B2%BE%E9%80%89&clientType=1"
  print(URLDecoder.decode(a))
  }

}
