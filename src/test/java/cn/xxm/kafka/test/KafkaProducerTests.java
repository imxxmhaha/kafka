package cn.xxm.kafka.test;

import cn.xxm.kafka.config.Message;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.*;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.security.Key;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@RunWith(SpringRunner.class)
@SpringBootTest
public class KafkaProducerTests {

    public final static String TOPIC_NAME = "xxm_topic";

    /**
     * topic描述信息
     */
    @Test
    public void producerSend() throws ExecutionException, InterruptedException {
        Producer<String, String> producer = getStringStringProducer();

        for (int i = 0; i <10 ; i++) {
            String key = "key-"+i;
            String message = getMessage();
            // 消息对象  - ProducerRecoder
            ProducerRecord<String, String> recode = new ProducerRecord<String, String>(TOPIC_NAME, key,message);
            Future<RecordMetadata> future = producer.send(recode);
            RecordMetadata recordMetadata = future.get();
            System.out.println("key:"+key+ " partition = " + recordMetadata.partition() +", offset:"+recordMetadata.offset());
        }

        // 所有通道打开都需要关闭
        producer.close();

    }


    /**
     * topic描述信息
     */
    @Test
    public void producerSendWithCallBack() throws ExecutionException, InterruptedException {
        Producer<String, String> producer = getStringStringProducer();

        for (int i = 0; i <10 ; i++) {
            String key = "key-"+i;
            String message = getMessage();
            // 消息对象  - ProducerRecoder
            ProducerRecord<String, String> recode = new ProducerRecord<String, String>(TOPIC_NAME, key,message);
            producer.send(recode, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    System.out.println("key:"+key+ " partition = " + recordMetadata.partition() +", offset:"+recordMetadata.offset());
                }
            });
        }

        // 所有通道打开都需要关闭
        producer.close();

    }


    private Producer<String, String> getStringStringProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"114.67.203.88:9092");
        properties.put(ProducerConfig.ACKS_CONFIG,"all");
        properties.put(ProducerConfig.RETRIES_CONFIG,"0");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,"16384");
        properties.put(ProducerConfig.LINGER_MS_CONFIG,"1");
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,"33554432");


        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        // Producer的主对象
        return new KafkaProducer<>(properties);
    }

    private String getMessage() {
        Message message = new Message();
        message.setId("KFK_"+System.currentTimeMillis());
        message.setMsg(UUID.randomUUID().toString());
        message.setSendTime(new Date());
        String msg = JSONObject.toJSONString(message);
        return msg;
    }


}
