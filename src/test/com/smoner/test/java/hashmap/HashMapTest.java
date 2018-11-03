package com.smoner.test.java.hashmap;

import java.util.HashMap;
import java.util.UUID;

/**
 * Created by fengjqc on 2018/11/3.
 */
public class HashMapTest {
    public static void main(String[] args){
        try{
            final HashMap<String,String> map = new HashMap<String,String>(2);
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    for(int i = 0; i<10000 ;i++){
                        new Thread(new Runnable() {
                            @Override
                            public void run() {
                                map.put(UUID.randomUUID().toString(),"");
                            }
                        },"ftf"+i).start();
                    }
                }
            },"ftf");
            t.start();
            t.join();
        }catch (InterruptedException e){
            e.printStackTrace();
            String s = null;
        }catch (Exception e){
            e.printStackTrace();
            String s = null;
        }
    }
}
