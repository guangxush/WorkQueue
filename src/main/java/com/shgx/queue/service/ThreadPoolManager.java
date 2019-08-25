package com.shgx.queue.service;

import com.shgx.queue.utils.RedisUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.*;

/**
 * @author: guangxush
 * @create: 2019/08/25
 */
@Service
@Slf4j
public class ThreadPoolManager implements BeanFactoryAware {

    private BeanFactory factory;

    /**
     * 线程池维护线程的最少数量
     */
    private final static int CORE_POOL_SIZE = 2;

    /**
     * 线程池维护线程的最大数量
     */
    private final static int MAX_POOL_SIZE = 10;

    /**
     * 线程池维护线程所允许的空闲时间
     */
    private final static int KEEP_ALIVE_TIME = 0;

    /**
     * 线程池所使用的缓冲队列大小
     */
    private final static int WORK_QUEUE_SIZE = 50;

    private final static boolean IS_QUEUE = false;

    @Autowired
    private WorkProducerService producerService;

    @Autowired
    private RedisUtils redisUtils;

    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        factory = beanFactory;
    }

    /**
     * 用于储存在队列中的订单,防止重复提交,在真实场景中，可用redis代替 验证重复
     */
    Map<String, Object> cacheMap = new ConcurrentHashMap<>();

    /**
     * 订单的缓冲队列,当线程池满了，则将订单存入到此缓冲队列
     */
    Queue<Object> msgQueue = new LinkedBlockingQueue<>();

    /**
     * 当线程池的容量满了，执行下面代码，将订单存入到缓冲队列
     */
    final RejectedExecutionHandler handler = new RejectedExecutionHandler() {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            if (IS_QUEUE) {
                //订单加入到缓冲队列
                msgQueue.offer(((ThreadPoolService) r).getBusinessNo());
            } else {
                //增加并发量，订单加入到kafka消息队列
                producerService.sendMessage(((ThreadPoolService) r).getBusinessNo());
            }
            log.info("系统任务已满,把此订单交给(调度线程池)逐一处理，订单号：" + ((ThreadPoolService) r).getBusinessNo());
        }
    };


    /**
     * 创建线程池
     */
    final ThreadPoolExecutor threadPool = new ThreadPoolExecutor(CORE_POOL_SIZE, MAX_POOL_SIZE, KEEP_ALIVE_TIME, TimeUnit.SECONDS, new ArrayBlockingQueue(WORK_QUEUE_SIZE), this.handler);

    /**
     * 将任务加入订单线程池
     */
    public void addOrders(String orderId) {
        log.info("此订单准备添加到线程池，订单号：" + orderId);
        //验证当前进入的订单是否已经存在
//        if (cacheMap.get(orderId) == null) {
//            cacheMap.put(orderId, new Object());
//            ThreadPoolService businessThread = new ThreadPoolService(orderId);
//            threadPool.execute(businessThread);
//        }
        //采用Redis缓存替代
        if (redisUtils.getSetMembers(orderId) == null) {
            redisUtils.addSetMembers(orderId, new Object());
            ThreadPoolService businessThread = new ThreadPoolService(orderId);
            threadPool.execute(businessThread);
        }
    }

    /**
     * 获取目前的活跃线程数量
     * @return
     */
    public int getActiveCount(){
        return threadPool.getActiveCount();
    }

    /**
     * 线程池的定时任务----> 称为(调度线程池)。此线程池支持定时以及周期性执行任务的需求。
     */
    final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(5);


    /**
     * 检查（调度线程池），每秒执行一次，查看订单的缓冲队列是否有订单记录，则重新加入到线程池
     */
    final ScheduledFuture scheduledFuture = scheduler.scheduleAtFixedRate(new Runnable() {
        @Override
        public void run() {
            //判断缓冲队列是否存在记录
            if (!msgQueue.isEmpty()) {
                //当线程池的队列容量少于workQueueSize，则开始把缓冲队列的订单加入到线程池
                if (threadPool.getQueue().size() < WORK_QUEUE_SIZE) {
                    String orderId = (String) msgQueue.poll();
                    ThreadPoolService businessThread = new ThreadPoolService(orderId);
                    threadPool.execute(businessThread);
                    log.info("(调度线程池)缓冲队列出现订单业务，重新添加到线程池，订单号：" + orderId);
                }
            }
        }
    }, 0, 1, TimeUnit.SECONDS);


    /**
     * 获取消息缓冲队列
     */
    public Queue<Object> getMsgQueue() {
        return msgQueue;
    }

    /**
     * 终止订单线程池+调度线程池
     */
    public void shutdown() {
        //true表示如果定时任务在执行，立即中止，false则等待任务结束后再停止
        log.info("终止订单线程池+调度线程池：" + scheduledFuture.cancel(false));
        scheduler.shutdown();
        threadPool.shutdown();
    }
}
