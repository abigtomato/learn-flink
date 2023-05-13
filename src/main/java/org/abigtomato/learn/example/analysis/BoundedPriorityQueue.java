package org.abigtomato.learn.example.analysis;

import com.google.common.collect.MinMaxPriorityQueue;

import java.util.Comparator;

/**
 * 有界优先队列
 *
 * @param <E>
 * @author abigtomato
 */
public class BoundedPriorityQueue<E> {

    private final MinMaxPriorityQueue<E> minMaxPriorityQueue;
    private final Integer size;

    public BoundedPriorityQueue(int size, Comparator<? super E> comparator) {
        this.size = size;
        this.minMaxPriorityQueue = MinMaxPriorityQueue
                .orderedBy(comparator)
                .expectedSize(size)
                .create();
    }

    public void add(E e) {
        this.minMaxPriorityQueue.offer(e);
        if (this.minMaxPriorityQueue.size() > this.size) {
            this.minMaxPriorityQueue.pollLast();
        }
    }

    public E get() {
        return this.minMaxPriorityQueue.pollFirst();
    }

    public static void main(String[] args) {
        int[] arr = {2, 17, 4, 12, 8, 21, 15, 33, 124, 55, 14, 11, 45};
        BoundedPriorityQueue<Integer> bq = new BoundedPriorityQueue<>(10, (a, b) -> b - a);
        for (int j : arr) {
            bq.add(j);
        }
        while (true) {
            Integer poll = bq.get();
            if (poll == null) {
                break;
            }
            System.out.println("poll = " + poll);
        }
    }
}
