package com.yt.base;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;


public class SyncProducer {
    public static void main(String[] args) throws MQClientException, InterruptedException {
        final DefaultMQProducer producer = new DefaultMQProducer("test-producer");
        producer.setNamesrvAddr("10.211.55.4:9876");
        producer.start();

        for (int i = 0; i < 10; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true){
                        try {
                            Message msg = new Message("TopicTest", "TagA", "Test".getBytes(RemotingHelper.DEFAULT_CHARSET));
                            SendResult sendResult = producer.send(msg);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }).start();
        }
        //程序卡在这里，不能让他结束，就不停发消息
        while (true){
            Thread.sleep(1000);
        }
    }
}
