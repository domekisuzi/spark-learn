package cn.xdc.bigdata.scala.core;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.JSONReader;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class jsonTest {

    public static void main(String[] args) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("s", "q");
        jsonObject.put("b", "asda");



        test1 test1 = JSONObject.parseObject(jsonObject.toString(), test1.class);


        List<test1> list = new ArrayList<>();
        list.add(new test1("q", "bc"));
        list.add(new test1("q", "bc"));
        JSONArray objects = new JSONArray();
//
        objects.add(new test1("a", "b"));
        objects.add(new test1("a", "b"));
        objects.add(new test1("a", "b"));
        objects.add(new test1("a", "b"));
        objects.add(new test1("a", "b"));
        //解析json数组
        List<cn.xdc.bigdata.scala.core.test1> test1s = JSON.parseArray(objects.toString(), cn.xdc.bigdata.scala.core.test1.class);
        System.out.println(test1s.toString());

//        System.out.println(objects.toJSONString());

//        JSONObject js = new JSONObject();
//        System.out.println(objects.toString());
//        List<cn.xdc.bigdata.scala.core.test1> list1 = js.getList(objects.toString(), cn.xdc.bigdata.scala.core.test1.class);
//        System.out.println(list1);


//        System.out.println(Range.BAD);
//
//        ExecutorService executorService = Executors.newFixedThreadPool(1);
//        CompletableFuture<String> c = CompletableFuture.supplyAsync(new Supplier<String>() {
//            @Override
//            public String get() {
//                //做处理
//                try {
//                    Thread.sleep(1000);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//                return "hello";
//            }
//        }, executorService);
//
//
//        c.thenAccept(
//                new Consumer<String>() {
//                    @Override
//                    public void accept(String s) {
//                        System.out.println(s);
//                        executorService.shutdown();
//                    }
//                }
//        );
//        System.out.println("会阻塞线程");
//        while (!executorService.isShutdown()) {
//            System.out.println("还在运行");
//            try {
//                Thread.sleep(100);
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }
//        }


    }
}
