package com.kdb.adapter.timer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class AdapterTimer {

  private ExecutorService executor = Executors.newSingleThreadExecutor();

  public Future<Boolean> tooLongSinceLastMsg(long waitTime) {
    return executor.submit(
        () -> {
          Thread.sleep(waitTime);
          return true;
        });
  }
}
