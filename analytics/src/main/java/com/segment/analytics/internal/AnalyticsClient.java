package com.segment.analytics.internal;

import com.segment.analytics.Callback;
import com.segment.analytics.http.SegmentService;
import com.segment.analytics.messages.Batch;
import com.segment.analytics.messages.Message;
import com.segment.backo.Backo;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import retrofit.RetrofitError;

import static com.segment.analytics.Log.Level.DEBUG;
import static com.segment.analytics.Log.Level.ERROR;
import static com.segment.analytics.Log.Level.VERBOSE;

public class AnalyticsClient {
  private static final Map<String, ?> CONTEXT;

  static {
    Map<String, String> library = new LinkedHashMap<>();
    library.put("name", "analytics-java");
    library.put("version", AnalyticsVersion.get());
    Map<String, Object> context = new LinkedHashMap<>();
    context.put("library", Collections.unmodifiableMap(library));
    CONTEXT = Collections.unmodifiableMap(context);
  }

  private final BlockingQueue<Message> messageQueue;
  private final SegmentService service;
  private final int size;
  private final Logger logger;
  private final List<Callback> callbacks;
  private final ExecutorService networkExecutor;
  private final ExecutorService looperExecutor;
  private final ScheduledExecutorService flushScheduler;

  public static AnalyticsClient create(SegmentService segmentService, int flushQueueSize,
      long flushIntervalInMillis, Logger logger, ThreadFactory threadFactory,
      ExecutorService networkExecutor, List<Callback> callbacks) {
    return new AnalyticsClient(new LinkedBlockingQueue<Message>(), segmentService, flushQueueSize,
        flushIntervalInMillis, logger, threadFactory, networkExecutor, callbacks);
  }

  AnalyticsClient(BlockingQueue<Message> messageQueue, SegmentService service, int maxQueueSize,
      long flushIntervalInMillis, Logger logger, ThreadFactory threadFactory,
      ExecutorService networkExecutor, List<Callback> callbacks) {
    this.messageQueue = messageQueue;
    this.service = service;
    this.size = maxQueueSize;
    this.logger = logger;
    this.callbacks = callbacks;
    this.looperExecutor = Executors.newSingleThreadExecutor(threadFactory);
    this.networkExecutor = networkExecutor;

    looperExecutor.submit(new Looper());

    flushScheduler = Executors.newScheduledThreadPool(1, threadFactory);
    flushScheduler.scheduleAtFixedRate(new Runnable() {
      @Override public void run() {
        flush();
      }
    }, flushIntervalInMillis, flushIntervalInMillis, TimeUnit.MILLISECONDS);
  }

  public void enqueue(Message message) {
    try {
      messageQueue.put(message);
    } catch (InterruptedException e) {
      logger.log(ERROR, "Interrupted with %s while adding message %s.", e, message);
      for (Callback callback : callbacks) {
        callback.failure(message, e);
      }
    }
  }

  public void flush() {
    enqueue(FlushMessage.POISON);
  }

  public void shutdown() {
    messageQueue.clear();
    looperExecutor.shutdownNow();
    flushScheduler.shutdownNow();
    networkExecutor.shutdown(); // Let in-flight requests complete.
  }

  /**
   * Looper runs on a background thread and takes messages from the queue. Once it collects enough
   * messages, it triggers a flush.
   */
  class Looper implements Runnable {
    @Override public void run() {
      List<Message> messages = new ArrayList<>();
      try {
        //noinspection InfiniteLoopStatement
        while (true) {
          Message message = messageQueue.take();

          if (message != FlushMessage.POISON) {
            messages.add(message);
          } else if (messages.size() < 1) {
            logger.log(VERBOSE, "No messages to flush.");
            continue;
          }

          if (messages.size() >= size || message == FlushMessage.POISON) {
            Batch batch = Batch.create(CONTEXT, messages);
            logger.log(VERBOSE, "Batching %s message(s) into batch %s.", messages.size(),
                batch.sequence());
            networkExecutor.submit(BatchUploadTask.create(AnalyticsClient.this, batch));
            messages = new ArrayList<>();
          }
        }
      } catch (InterruptedException e) {
        logger.log(DEBUG, "Looper interrupted while polling for messages %s.", e);
      }
    }
  }

  static class BatchUploadTask implements Runnable {
    private static final Backo BACKO = Backo.builder() //
        .base(TimeUnit.SECONDS, 15) //
        .cap(TimeUnit.HOURS, 1) //
        .jitter(1) //
        .build();
    private static final int MAX_ATTEMPTS = 50; // Max 50 hours ~ 2 days

    private final AnalyticsClient client;
    private final Backo backo;
    final Batch batch;

    static BatchUploadTask create(AnalyticsClient client, Batch batch) {
      return new BatchUploadTask(client, BACKO, batch);
    }

    BatchUploadTask(AnalyticsClient client, Backo backo, Batch batch) {
      this.client = client;
      this.batch = batch;
      this.backo = backo;
    }

    /** Returns {@code true} to indicate a batch should be retried. {@code false} otherwise. */
    boolean upload() {
      try {
        client.logger.log(VERBOSE, "Uploading batch %s.", batch.sequence());

        // Ignore return value, UploadResponse#onSuccess will never return false for 200 OK
        client.service.upload(batch);

        client.logger.log(VERBOSE, "Uploaded batch %s.", batch.sequence());
        for (Message message : batch.batch()) {
          for (Callback callback : client.callbacks) {
            callback.success(message);
          }
        }
        return false;
      } catch (RetrofitError error) {
        switch (error.getKind()) {
          case NETWORK:
            client.logger.log(DEBUG, "Could not upload batch %s with error %s. Retrying.",
                batch.sequence(), error);
            return true;
          default:
            client.logger.log(ERROR, "Could not upload batch %s with error. Giving up.",
                batch.sequence(), error);
            for (Message message : batch.batch()) {
              for (Callback callback : client.callbacks) {
                callback.failure(message, error);
              }
            }
            return false; // Don't retry
        }
      }
    }

    @Override public void run() {
      for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
        boolean retry = upload();
        if (!retry) return;
        try {
          backo.sleep(attempt);
        } catch (InterruptedException e) {
          client.logger.log(DEBUG, "Thread interrupted while backing off for batch %s.",
              batch.sequence());
          return;
        }
      }

      client.logger.log(ERROR, "Could not upload batch %s. Retries exhausted.", batch.sequence());
      IOException exception = new IOException(MAX_ATTEMPTS + " retries exhausted");
      for (Message message : batch.batch()) {
        for (Callback callback : client.callbacks) {
          callback.failure(message, exception);
        }
      }
    }
  }
}
