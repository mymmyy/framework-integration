package com.mym.sparkproject.spark.session;

import scala.math.Ordered;

import java.io.Serializable;

/**
 * 品类二次排序key
 *
 * 封装你想要继续宁排序算法需要的几个字段：点击次数、下单次数、支付次数
 * 实现Ordered接口需要的几个方法
 *
 * 跟其他Key相比，如何判断>、>=、<、=<
 * 依次使用三个次数进行比较，如果某一个相等，那么就比较下一个
 *
 */
public class CategorySortKey implements Ordered<CategorySortKey>, Serializable{
    private static final long serialVersionUID = -6007890914324789180L;

    private long clickCount;
    private long orderCount;
    private long payCount;

    public CategorySortKey(long clickCount, long orderCount, long payCount){
        this.clickCount = clickCount;
        this.orderCount = orderCount;
        this.payCount = payCount;
    }

    @Override
    public int compare(CategorySortKey other) {
        if(clickCount - other.getClickCount() != 0) {
            return (int) (clickCount - other.getClickCount());
        } else if(orderCount - other.getOrderCount() != 0) {
            return (int) (orderCount - other.getOrderCount());
        } else if(payCount - other.getPayCount() != 0) {
            return (int) (payCount - other.getPayCount());
        }
        return 0;
    }

    @Override
    public boolean $less(CategorySortKey other) {
        if(clickCount < other.getClickCount()) {
            return true;
        } else if(clickCount == other.getClickCount() &&
                orderCount < other.getOrderCount()) {
            return true;
        } else if(clickCount == other.getClickCount() &&
                orderCount == other.getOrderCount() &&
                payCount < other.getPayCount()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater(CategorySortKey that) {
        if(clickCount > that.getClickCount()){
            return true;
        }else if(clickCount == that.getClickCount() && orderCount > that.getOrderCount()){
            return true;
        }else if(clickCount == that.getClickCount() && orderCount == that.getOrderCount() && payCount > that.getPayCount()){
            return true;
        }
        return false;
    }

    @Override
    public boolean $less$eq(CategorySortKey other) {
        if(clickCount < other.getClickCount()) {
            return true;
        } else if(clickCount == other.getClickCount() &&
                orderCount < other.getOrderCount()) {
            return true;
        } else if(clickCount == other.getClickCount() &&
                orderCount == other.getOrderCount() &&
                payCount < other.getPayCount()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater$eq(CategorySortKey other) {
        if($greater(other)) {
            return true;
        } else if(clickCount == other.getClickCount() &&
                orderCount == other.getOrderCount() &&
                payCount == other.getPayCount()) {
            return true;
        }
        return false;
    }

    @Override
    public int compareTo(CategorySortKey other) {
        if(clickCount - other.getClickCount() != 0) {
            return (int) (clickCount - other.getClickCount());
        } else if(orderCount - other.getOrderCount() != 0) {
            return (int) (orderCount - other.getOrderCount());
        } else if(payCount - other.getPayCount() != 0) {
            return (int) (payCount - other.getPayCount());
        }
        return 0;
    }

    public long getClickCount() {
        return clickCount;
    }

    public long getOrderCount() {
        return orderCount;
    }

    public long getPayCount() {
        return payCount;
    }
}
