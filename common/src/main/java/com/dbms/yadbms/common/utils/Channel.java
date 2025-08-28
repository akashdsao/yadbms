package com.dbms.yadbms.common.utils;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A simple thread-safe channel implementation using a queue. This class allows multiple producers
 * to put elements into the channel and multiple consumers to get elements from the channel.
 *
 * @param <T> the type of elements in the channel
 */
public class Channel<T> {

  private final Queue<T> q;

  private final ReentrantLock channelIOLock;

  private final Condition notEmptyConditionVariable;

  public Channel() {
    q = new LinkedList<>();
    channelIOLock = new ReentrantLock();
    notEmptyConditionVariable = channelIOLock.newCondition();
  }

  /**
   * Puts an element into the channel. If the channel is empty, it signals waiting consumers.
   *
   * @param element the element to put into the channel
   */
  public void put(T element) {
    channelIOLock.lock();
    try {
      q.add(element);
      notEmptyConditionVariable.signalAll(); // Signal consumer
    } finally {
      channelIOLock.unlock();
    }
  }

  /**
   * Gets an element from the channel. If the channel is empty, it waits until an element is
   * available.
   *
   * @return the element from the channel
   * @throws InterruptedException if the thread is interrupted while waiting
   */
  public T get() throws InterruptedException {
    channelIOLock.lock();
    try {
      while (q.isEmpty()) {
        notEmptyConditionVariable.await(); // Wait until there's data
      }
      return q.poll();
    } finally {
      channelIOLock.unlock();
    }
  }

  /**
   * Checks if the channel is empty.
   *
   * @return true if the channel is empty, false otherwise
   */
  public boolean isEmpty() {
    channelIOLock.lock();
    try {
      return q.isEmpty();
    } finally {
      channelIOLock.unlock();
    }
  }
}
