package com.yt.transaction;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.tomcat.jni.Local;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.*;

public class TransactionProducer {

    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException {

        //用来接收RocketMQ的回调的监听器
        //这里会实现本地事务 commit rollback 及回调查询等
        TransactionListener listener = new TransactionListener() {

            //如果half发送成功了，就会回调此方法，执行本地事务，commit or rollback
            @Override
            public LocalTransactionState executeLocalTransaction(Message message, Object o) {
                try{
                    //执行订单事务
                    return LocalTransactionState.COMMIT_MESSAGE;
                }catch (Exception e){
                    //进行roll back
                    return LocalTransactionState.ROLLBACK_MESSAGE;
                }
            }

            //如果由于各种原因没有commit或者rollback，则进行回调查询，让你去rollback或者commit
            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {

                //查询本地事务是否执行成功
                int status = 0;
                //int status = localTrans.get(messageExt.getThansactionId);
                //根据本地事务执行commit 或者 rollback
                switch (status){
                    case 1: return LocalTransactionState.UNKNOW;
                    case 2: return LocalTransactionState.COMMIT_MESSAGE;
                    case 3: return LocalTransactionState.ROLLBACK_MESSAGE;
                }
                return LocalTransactionState.COMMIT_MESSAGE;
            }
        };

        //创建一个支持事务的producer  指定一个生产者分组名字
        TransactionMQProducer transactionProducer = new TransactionMQProducer("testMQGroup");

        //指定一个线程池，他的作用是用来处理MQ回调你的请求的
        ExecutorService executorService = new ThreadPoolExecutor(
                2,
                5,
                100,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(2000),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread thread = new Thread(r);
                        thread.setName("testThread");
                        return thread;
                    }
                }
        );

        //给事务消息设置对应的线程池，负责执行MQ回调请求
        transactionProducer.setExecutorService(executorService);

        transactionProducer.setTransactionListener(listener);
        transactionProducer.start();

        Message message = new Message("OrderTopic", "OrderTag", "TestKey", ("订单支付消息").getBytes(RemotingHelper.DEFAULT_CHARSET));

        try {
            TransactionSendResult sendResult = transactionProducer.sendMessageInTransaction(message, null);
        }catch (Exception e){
            //如果出现异常，则证明half消息 发送失败
            //订单系统执行回滚逻辑，比如支付退款，更新状态为已关闭
        }
        //如果没有收到half消息的执行成功的通知？
        //我们可以把发送的half存到内存或写入本地磁盘，后台开启一个线程去检查，如果一个half消息10分钟都没有响应，则执行回滚逻辑

    }
}
