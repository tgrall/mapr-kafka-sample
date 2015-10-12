package com.mapr.db.sample.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer ;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import twitter4j.FilterQuery;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStreamFactory;
import twitter4j.json.DataObjectFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class TwitterStream {
  private static String[] track = null;


  public TwitterStream() {

  }

  public static void main(String args[]) throws Exception {


    //setup twitter
    twitterSetup(args);

    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("request.required.acks", "1");
    injectTweets(props);



  }


  private static void injectTweets(Properties props) {

    final KafkaProducer producer = new KafkaProducer(props);
    twitter4j.TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
    final String topic = "TR069" ;

    try {
      StatusListener listener = new StatusListener() {

        public void onStatus(final Status status) {
          String twitterMessage = DataObjectFactory.getRawJSON(status);
          String msg = twitterMessage;

          try {

            ProducerRecord data = new ProducerRecord(topic,
                    Long.toString(status.getId()) , msg);

            Future rs = producer.send(data, new Callback() {
              public void onCompletion(RecordMetadata recordMetadata, Exception e) {

                System.out.println  ("ack for "+ status.getUser().getScreenName());
              }
            });

            try {
              RecordMetadata rm = (RecordMetadata) rs.get();
              msg = msg + "  partition = " + rm.partition() +  " offset =" + rm.offset() ;
            } catch(Exception e) {
              System.out.println(e) ;
            }



          } catch (Exception e) {
            e.printStackTrace();
          }
        }

        public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
        }

        public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
        }

        public void onScrubGeo(long userId, long upToStatusId) {
        }

        public void onException(Exception ex) {
          ex.printStackTrace();
        }
      };

      twitterStream.addListener(listener);
      if (track == null) {
        twitterStream.sample();
      } else {
        long[] follow = {};
        FilterQuery filter = new FilterQuery(0, follow, track );
        twitterStream.filter(filter);
      }


    } catch (Exception e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }


  private static void twitterSetup(String[] args) throws Exception {
    Properties prop = new Properties();
    InputStream in = TwitterStream.class.getClassLoader().getResourceAsStream("twitter4j.properties");
    if (in == null) {
      throw new Exception("File twitter4j.properties not found");
    }
    prop.load(in);
    in.close();


    if (args.length != 0 ) {
      track = args[0].split(",");
    }
  }


}
