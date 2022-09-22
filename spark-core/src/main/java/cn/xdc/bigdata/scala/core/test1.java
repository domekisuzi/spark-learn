package cn.xdc.bigdata.scala.core;

import com.alibaba.fastjson2.annotation.JSONField;

import java.io.Serializable;

public class test1 implements Serializable {
    @JSONField(name = "s")
    private String s;

    @JSONField(name = "b")
    private String b;

    public test1(String s, String b) {
        this.s = s;
        this.b = b;
    }

    public String getS() {
        return s;
    }

    public void setS(String s) {
        this.s = s;
    }

    public String getB() {
        return b;
    }

    public void setB(String b) {
        this.b = b;
    }

    @Override
    public String toString() {
        return "test1{" +
                "s='" + s + '\'' +
                ", b='" + b + '\'' +
                '}';
    }
}
