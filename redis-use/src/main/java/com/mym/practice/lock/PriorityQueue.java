package com.mym.practice.lock;

/**
 * 优先队列
 */
public class PriorityQueue {

    /**
     * 默认容量
     */
    private static final int DEFAULT_CAPACITY = 10;

    /**
     * 存数据的数组
     */
    private int[] array;

    /**
     * 当前大小（实际大小）
     */
    private int currentSize;

    public PriorityQueue(){
        array = new int[DEFAULT_CAPACITY];
    }

    public PriorityQueue(int capacity){
        array = new int[capacity];
    }

    public PriorityQueue(int[] arr){
        currentSize = arr.length;
        this.array = new int[currentSize * 2];

        // 舍弃0位，方便二叉堆计算
        for(int i = 1; i < currentSize + 1; i++){
            array[i] = arr[i - 1];
        }

        // 构建初始堆
        buildHeap();
    }

    /**
     * 入队列
     * @param x 入队列数据
     */
    public void offer(int x){
        if(currentSize == array.length - 1){
            // 需要扩容
            enlargeArray(array.length * 2 + 1);
        }

        // 当前空位
        int hole = ++currentSize;
        // 重新调整为堆结构
        for(array[0] = x; x < array[hole / 2]; hole /= 2){
            array[hole] = array[hole / 2];
        }
        // 入队列，并删除临时使用的
        array[hole] = x;
        array[0] = 0;
    }

    /**
     * 出队列
     * @return
     */
    public int poll(){
        int result = array[1];
        // 更新
        array[1] = array[currentSize];
        array[currentSize] = 0;
        currentSize--;
        // 重新调整
        buildHeap();
        return result;
    }

    /**
     * 下滤，下沉
     * @param hole 当前位置
     */
    private void percolateDown(int hole){
        int child;
        int temp = array[hole];
        while(hole << 1 <= currentSize){
            child = hole << 1;      // 左孩。右孩节点 = 左孩节点 + 1
            if(child != currentSize && array[child + 1] < array[child]){
                child++;
            }
            if(array[child] < temp){
                array[hole] = array[child];
            }else{
                break;
            }
            hole = child;
        }
        array[hole] = temp;
    }

    /**
     * 构建堆
     */
    private void buildHeap(){
        for(int i = currentSize / 2; i > 0; i--){
            percolateDown(i);
        }
    }

    /**
     * 扩展数组
     * @param newSize
     */
    private void enlargeArray(int newSize){
        if(newSize <= array.length){
            return;
        }
        int[] ints = new int[newSize];
        for(int i = 0; i < array.length; i++){
            ints[i] = array[i];
        }
        array = ints;
    }

    /**
     * 删除最小的
     * @return 删除的数值
     */
    public int deleteMin(){
        return poll();
    }

    /**
     * 打印数组结构
     */
    public void print(){
        for(int i = 0; i < array.length; i++){
            System.out.print(array[i] + " ");
        }
        System.out.println();
    }

    public static void main(String[] args) {
        int[] arr = {3,6,4,5,7};
        PriorityQueue priorityQueue = new PriorityQueue(arr);
        priorityQueue.print();

        int poll = priorityQueue.poll();
        System.out.println("出队列的是:" + poll);
        System.out.println("现堆数组情况:");
        priorityQueue.print();

        int min = priorityQueue.deleteMin();
        System.out.println("删除当前最小的元素是:" + min);
        System.out.println("现堆数组情况:");
        priorityQueue.print();

        priorityQueue.offer(3);
        System.out.println("入队列数后，现堆数组情况:");
        priorityQueue.print();
    }
}
