package com.blokaly.ceres.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Spliterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;

public final class EventQueueSpliterator<T> implements Spliterator<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventQueueSpliterator.class);
    private static final int DEFAULT_CAPACITY = 128;
    private final BlockingQueue<T> queue;

    public EventQueueSpliterator() {
        this(new ArrayBlockingQueue<>(DEFAULT_CAPACITY));
    }

    public EventQueueSpliterator(BlockingQueue<T> queue) {
        this.queue = queue;
    }

    @Override
    public boolean tryAdvance(Consumer<? super T> action) {
        try {
            action.accept(queue.take());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return true;
    }

    @Override
    public Spliterator<T> trySplit() {
        return null;
    }


    @Override
    public long estimateSize() {
        return Long.MAX_VALUE;
    }

    @Override
    public int characteristics() {
        return  Spliterator.CONCURRENT | Spliterator.NONNULL | Spliterator.ORDERED;
    }

    public void add(T event) {
        if (event == null) {
            return;
        }

        boolean success = queue.offer(event);
        if (!success) {
            LOGGER.error("Failed to add event to queue: {}", event);
        }
    }
}
