package com.shgx.queue.controller;

import com.shgx.queue.service.ThreadPoolManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.Queue;
import java.util.UUID;

/**
 * @author: guangxush
 * @create: 2019/08/25
 */
@RestController
public class BusinessController {

    @Autowired
    private ThreadPoolManager threadPoolManager;
    /**
     * 测试模拟下单请求 入口
     * @param id
     * @return
     */
    @GetMapping("/start/{id}")
    public String start(@PathVariable Long id) {
        //模拟的随机数
        String orderNo = System.currentTimeMillis() + UUID.randomUUID().toString();

        threadPoolManager.addOrders(orderNo);

        return "Test ThreadPoolExecutor start";
    }

    /**
     * 停止服务
     * @param id
     * @return
     */
    @GetMapping("/end/{id}")
    public String end(@PathVariable Long id) {

        threadPoolManager.shutdown();

        Queue q = threadPoolManager.getMsgQueue();
        System.out.println("关闭了线程服务，还有未处理的信息条数：" + q.size());
        return "Test ThreadPoolExecutor start";
    }
}
