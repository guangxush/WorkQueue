package com.shgx.queue.service;

/**
 * @author: guangxush
 * @create: 2019/08/24
 */

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope("prototype")
public class ThreadPoolService implements Runnable{

    private String businessNo;

    public ThreadPoolService(String businessNo) {
        this.businessNo = businessNo;
    }

    public String getBusinessNo() {
        return businessNo;
    }

    public void setBusinessNo(String businessNo) {
        this.businessNo = businessNo;
    }

    @Override
    public void run() {
        //业务操作
        System.out.println("多线程已经处理订单插入系统，订单号："+ businessNo);

        //线程阻塞
        /*try {
            Thread.sleep(1000);
            System.out.println("多线程已经处理订单插入系统，订单号："+businessNo);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }*/
    }
}
