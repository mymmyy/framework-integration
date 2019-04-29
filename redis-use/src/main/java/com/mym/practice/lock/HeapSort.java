package com.mym.practice.lock;

/**
 * 堆排序测试类
 */
public class HeapSort {


    /**
     * 调整为堆结构：从最后一个子树的根节点位置开始调整
     * @param arr 数组
     * @param lastRootIndex 最后一个子树的根节点位置
     * @param endIndex 本次初始化堆结束位置
     */
    public static void adjustHeap(int[] arr, int lastRootIndex, int endIndex){
        // 下标从1开始
        if(arr.length < endIndex && lastRootIndex < 1){
            return;
        }
        int leftNodeIndex;
        int rightNodeIndex;
        for(int i = lastRootIndex;i > 0;i--){
            leftNodeIndex = i << 1;
            rightNodeIndex = (i << 1) + 1;
            if(rightNodeIndex > endIndex){
                rightNodeIndex = i;
            }
            int temp = arr[i];
            if(arr[rightNodeIndex] < arr[leftNodeIndex] && temp < arr[leftNodeIndex]){
                arr[i] = arr[leftNodeIndex];
                arr[leftNodeIndex] = temp;
            }else if(arr[rightNodeIndex] > arr[leftNodeIndex] && arr[rightNodeIndex] > temp){
                arr[i] = arr[rightNodeIndex];
                arr[rightNodeIndex] = temp;
            }
        }

    }

    /**
     * 初始化堆：遍历每个局部子树进行构造堆结构
     * @param arr 数组
     * @param endIndex 本次初始化堆结束位置
     */
    private static void initHeap(int[] arr, int endIndex){
        for(int i = 1; i < endIndex; i++){
            if(i << 1 > endIndex){
                break;
            }
            adjustHeap(arr, i, endIndex);
        }
    }

    /**
     * 打印一个数组
     * @param arr 数组
     */
    private static void print(int[] arr){
        for(int i = 0; i < arr.length; i++){
            System.out.print(arr[i] + " ");
        }
        System.out.println();
    }

    public static void main(String[] args) {
        // 注意：index=0位不用管，方便计算。真实需要排序的数从index=1开始
        int[] arr = {0,3,6,4,5,7,8};

        // 构造初始堆：最大堆
        initHeap(arr, arr.length - 1);
        System.out.println("初始堆：");
        print(arr);
        // 进行堆排序
        for(int i = arr.length - 1; i > 1; i--){
            // 1.最后一个未排序的与第一位交换
            int temp = arr[i];
            arr[i] = arr[1];
            arr[1] = temp;
            // 2.经过交换后需要重新调整为堆结构（已排序完的数据不参与）
            initHeap(arr, i - 1);
        }
        System.out.println("排序结果：");
        print(arr);
    }


}
