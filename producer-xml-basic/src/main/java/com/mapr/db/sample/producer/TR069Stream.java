package com.mapr.db.sample.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import twitter4j.FilterQuery;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStreamFactory;
import twitter4j.json.DataObjectFactory;

import java.io.InputStream;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

public class TR069Stream {
  private static String[] track = null;


  public TR069Stream() {

  }

  public static void main(String args[]) throws Exception {


    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("request.required.acks", "1");
    injectMessages(props);



  }


  private static void injectMessages(Properties props) throws InterruptedException {

    final KafkaProducer producer = new KafkaProducer(props);
    final String topic = "TR069";


    for (; ; ) {


      int deviceId = ThreadLocalRandom.current().nextInt(100000);
      int ip = ThreadLocalRandom.current().nextInt(255);
      int ip2 = ThreadLocalRandom.current().nextInt(20);

      // generate log
      String log =
             "<log>\n" +
                     " <device_id>"+ deviceId +"</device_id>\n" +
                     " <ip>123.124.200."+ ip2 +"</ip>\n" +
                     " <ts>"+ new Date() +"</ts>\n" +
                     " <uuid>"+ UUID.randomUUID().toString() +"</uuid>\n" +
                     "</log>";
      Thread.sleep(800);


      try {

        ProducerRecord data = new ProducerRecord(topic, UUID.randomUUID().toString()
                , log);

        Future rs = producer.send(data, new Callback() {
          public void onCompletion(RecordMetadata recordMetadata, Exception e) {

          }
        });

        try {
          RecordMetadata rm = (RecordMetadata) rs.get();
          System.out.print("|");
        } catch (Exception e) {
          System.out.println(e);
        }


      } catch (Exception e) {
        e.printStackTrace();
      }

    }

  }

}
