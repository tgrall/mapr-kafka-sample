package com.mapr.db.sample.consumer;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.json.JSONObject;
import org.json.XML;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConsumerXML2JSON {


  public static void main(String args[]) throws IOException {


    Properties props = new Properties();
    props.put("zookeeper.connect", "localhost:2181");
    props.put("group.id", "jsongroup");
    props.put("zookeeper.session.timeout.ms", "413");
    props.put("zookeeper.sync.time.ms", "203");
    props.put("auto.commit.interval.ms", "1000");
    ConsumerConfig cf = new ConsumerConfig(props) ;


    // open log
    String fileS = "/Volumes/mapr/demo.mapr.com/apps/logs/tr069/json/2015-10-08.json";
    Writer output = new BufferedWriter(new FileWriter(fileS, true));


    ConsumerConnector consumer = Consumer.createJavaConsumerConnector(cf) ;
    String topic = "TR069"  ;
    Map topicCountMap = new HashMap();
    topicCountMap.put(topic, new Integer(3));
    Map<String,List<KafkaStream<byte[],byte[]>>> consumerMap =
            consumer.createMessageStreams(topicCountMap);
    List<KafkaStream<byte[],byte[]>> streams = consumerMap.get(topic);

    ExecutorService executor = Executors.newFixedThreadPool(3); ;
    int threadnum = 0 ;
    for(KafkaStream stream  : streams) {
      executor.execute(new XMLToJSON(stream, threadnum, output));
      ++threadnum ;
    }

  }

}


class XMLToJSON implements Runnable {
  private KafkaStream m_stream;
  private int m_threadNumber;
  private Writer m_output;

  public XMLToJSON(KafkaStream a_stream, int a_threadNumber, Writer a_output) {
    m_threadNumber = a_threadNumber;
    m_stream = a_stream;
    m_output = a_output;
  }

  public void run() {
    ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
    while (it.hasNext()) {
      String xmlMessage = new String(it.next().message());

      System.out.print("*");
      JSONObject json = XML.toJSONObject(xmlMessage);

      try {
        m_output.append( json.get("log").toString() +"\n");
        m_output.flush();
      } catch (IOException e) {
        e.printStackTrace();
      }


    }
    System.out.println("Shutting down Thread: " + m_threadNumber);
  }
}



