package com.mym.sparkproject.test;

/**
 * 堆排序测试类
 */
public class HeapSort {


    public static void initHeap(int[] arr){
        // 下标从1开始
        if(arr.length < 3){
            return;
        }
        for(int i = 1;i < arr.length;i++){
            int temp = arr[i];
            if(arr[i << 1 + 1] < arr[i << 1] && temp < arr[i << 1]){
                arr[i] = arr[i << 1];
                arr[i << 1] = temp;
            }else if(arr[i << 1 + 1] > arr[i << 1] && arr[i << 1 + 1] > temp){
                arr[i] = arr[i << 1 + 1];
                arr[i << 1 + 1] = temp;
            }
            if(i % 2 == 0){
                i = i + 1;
            }else{
                i = i << 1;
            }
        }

    }

    private static void print(int[] arr){
        for(int i = 0; i < arr.length; i++){
            System.out.print(arr[i] + " ");
        }
        System.out.println();
    }

    public static void main(String[] args) {
        int[] arr = {3,6,4,5,7,8};
        initHeap(arr);
        print(arr);
    }


}
