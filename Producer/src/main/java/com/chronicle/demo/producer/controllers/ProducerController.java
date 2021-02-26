package com.chronicle.demo.producer.controllers;

import com.kdb.adapter.data.QuoteHelper;
import net.openhft.chronicle.wire.DocumentContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@RestController
public class ProducerController {

  private static Logger LOG = LoggerFactory.getLogger(ProducerController.class);

  @Value("${chronicle.quote.queue}")
  String quoteQueuePath;

  private boolean startQuoteGenerator = false;

  @GetMapping(value = "/quoteLoader")
  public String quoteLoader(
      @RequestParam(value = "Command: start/stop", required = true) String command,
      @RequestParam(value = "No. to generate", required = true) int num,
      @RequestParam(value = "Msg Interval in millis", required = true) long msgInterval,
      @RequestParam(value = "Batch Interval in millis", required = true) long batchInterval) {
    try {
      if ("START".equalsIgnoreCase(command)) {
        startQuoteGenerator = true;
        quoteGenerator(num, msgInterval, batchInterval);
      } else if ("STOP".equalsIgnoreCase(command)) {
        startQuoteGenerator = false;
      }
    } catch (Exception e) {
      return "*** Encountered error in method quoteLoader: " + e.getMessage();
    }
    LOG.info("*** Successfully executed query");
    return ("*** Successfully executed query");
  }

  public void quoteGenerator(int numToGenerate, long msgInterval, long batchInterval) {

    try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(quoteQueuePath).build();
        ExcerptAppender appender = queue.acquireAppender(); ) {

      QuoteHelper quoteHelper = new QuoteHelper();

      ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

      Runnable task = () -> generateMessages(numToGenerate, appender, quoteHelper, msgInterval);

      scheduler.scheduleWithFixedDelay(task, 0, batchInterval, TimeUnit.MILLISECONDS);

      while (true) {
        if (!startQuoteGenerator) {
          scheduler.shutdown();
          try {
            if (!scheduler.awaitTermination(800, TimeUnit.MILLISECONDS)) {
              scheduler.shutdownNow();
            }
          } catch (InterruptedException e) {
            scheduler.shutdownNow();
          }
          break;
        }
      }
      LOG.info("Out of while loop");
    }
  }

  public void generateMessages(
      long numToGenerate, ExcerptAppender appender, QuoteHelper quoteHelper, long msgInterval) {

    long numMsgsWritten = 0L;
    long start = System.nanoTime();

    while (numToGenerate == 0 || numMsgsWritten < numToGenerate) {
      // Check for pause between each msg. Only pause if config > 0
      if (msgInterval > 0) {
        try {
          Thread.sleep(msgInterval);
        } catch (InterruptedException ie) {
          LOG.error("Error with pause between messages: {}", ie.toString());
          break;
        }
      }

      try (DocumentContext dc = appender.writingDocument()) {
        dc.wire().write("quote").object(quoteHelper.generateQuoteMsg());
      } catch (Exception ex) {
        LOG.error("Error writing message: {}", ex.toString());
      }
      numMsgsWritten++;
    }

    long finish = System.nanoTime() - start;
    long index = appender.lastIndexAppended();

    LOG.info(
        "TIMING: Added {} messages (up to index: {}) in {} seconds",
        numMsgsWritten,
        index,
        finish / 1e9);
  }
}
