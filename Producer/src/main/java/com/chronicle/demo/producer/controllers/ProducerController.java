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
      @RequestParam(value = "Interval in millis", required = true) long interval) {
    try {
      if ("START".equalsIgnoreCase(command)) {
        startQuoteGenerator = true;
        quoteGenerator(num, interval);
      } else if ("STOP".equalsIgnoreCase(command)) {
        startQuoteGenerator = false;
      }
    } catch (Exception e) {
      return "*** Encountered error in method quoteLoader: " + e.getMessage();
    }
    LOG.info("*** Successfully executed query");
    return ("*** Successfully executed query");
  }

  public void quoteGenerator(int numToGenerate, long interval) throws InterruptedException {

    try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(quoteQueuePath).build();
        ExcerptAppender appender = queue.acquireAppender(); ) {

      long numMsgsWritten = 0L;
      long start = System.nanoTime();
      QuoteHelper quoteHelper = new QuoteHelper();

      while (startQuoteGenerator && (numToGenerate == 0 || numMsgsWritten < numToGenerate)) {
        // Only pause if config > 0
        if (interval > 0) {
          Thread.sleep(interval);
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
}
