## 背景
1. 在用户量比较高的情况下，会有很多请求过来，此时线程池处理能力已经无法满足需求，如何解决？
- 方案：可以将当前的计算线程先保存起来，放入高并发消息队列中，等线程池中的任务较少时，再从队列拉数据去执行任务。
2. 如果微服务框架下有服务被熔断或者降级，其他任务不能查询该服务的信息，又不能直接丢弃，如何解决？
- 方案：将当前任务暂时存放在消息队列中，等待服务恢复之后，在从消息队列中拉取任务执行。

## 执行流程
1. 当批量用户请求过来时，先把用户请求放入Redis Map中或者ConcurrentHashMap中，对用户请求去重，保证交易号幂等性；
2. 这里创建一个线程池处理用户请求；
- 如果当前线程池请求数目<核心线程池数目，直接让核心线程池去执行任务 
- 如果大于核心线程池数目，那么加入到线程队列中
- 如果队列已满，那么创建新的线程去处理，如果新的线程数目超过了最大线程数目，任务将被拒绝
- 拒绝策略是将任务提交到kafka消息队列中进行存储
3. 等到线程池中的任务较少或者夜间用户访问较少的时候，从消息队列中拉取请求重新进行处理
4. 如果处理失败将任务重新加入到消息队列中，等待一定的时机进行重试。
![项目框架](https://github.com/guangxush/iTechHeart/blob/master/image/WorkQueue/workqueue.png0)

## 代码实现

- 模拟线程处理用户请求
 ```
@Override
    public void run() {
        //业务操作
        System.out.println("多线程已经处理订单插入系统，订单号："+ businessNo);
    }
```
- 采用线程池处理以上请求
```
   @Autowired
    private WorkProducerService producerService;

    @Autowired
    private RedisUtils redisUtils;

    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        factory = beanFactory;
    }

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

```

将处理不了的请求，发送给消息队列暂存

```
public boolean sendMessage(String msg) {
        try {
            String result = kafkaTemplate.send("business", msg).get().toString();
            if (result != null) {
                return true;
            }
        } catch (Exception e) {
            return false;
        }
        return false;
    }
```

等到线程池中的请求能够处理当前任务时，消费消息并对请求进行处理(可以设置定时任务进行拉取)
```
@KafkaListener(id = "test-consumer-group", topics = "business",containerFactory = "ackContainerFactory")
    public void ackListener(ConsumerRecord record, Acknowledgment ack) {
        log.info("Receive Business Number :------------"+record.value());
        if(threadPoolManager.getActiveCount()< MAX_POOL_SIZE){
            threadPoolManager.addOrders(record.value().toString());
            ack.acknowledge();
        }else{
            //未被消费的消息重新发到队列
            workProducerService.sendMessage(record.value().toString());
        }
    }
```

## 源码参考

[并发任务调度](https://github.com/guangxush/WorkQueue)
