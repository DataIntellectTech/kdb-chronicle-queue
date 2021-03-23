package uk.co.aquaq.chronicle.producer.controllers;

import uk.co.aquaq.kdb.adapter.data.QuoteHelper;
import uk.co.aquaq.kdb.adapter.data.TradeHelper;
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
import java.util.concurrent.atomic.AtomicBoolean;

@RestController
public class ProducerController {

  private static Logger LOG = LoggerFactory.getLogger(ProducerController.class);

  @Value("${chronicle.quote.queue}")
  String quoteQueuePath;

  @Value("${chronicle.trade.queue}")
  String tradeQueuePath;

  private AtomicBoolean startQuoteGenerator = new AtomicBoolean(false);
  private AtomicBoolean startTradeGenerator = new AtomicBoolean(false);
  private int numberOfQuoteLoops = 0;
  private int numberOfTradeLoops = 0;

  @GetMapping(value = "/quoteLoader")
  public String quoteLoader(
          @RequestParam(value = "Command: start/stop", required = true) String command,
          @RequestParam(value = "Number of loops (0 = loop continuously)", required = true)
                  int numLoops,
          @RequestParam(value = "No. to generate", required = true) int num,
          @RequestParam(value = "Msg Interval in millis", required = true) long msgInterval,
          @RequestParam(value = "Batch Interval in millis", required = true) long batchInterval) {
    try {
      if ("START".equalsIgnoreCase(command)) {
        startQuoteGenerator.set(true);
        quoteGenerator(numLoops, num, msgInterval, batchInterval);
      } else if ("STOP".equalsIgnoreCase(command)) {
        startQuoteGenerator.set(false);
      }
    } catch (Exception e) {
      return "*** Encountered error in method quoteLoader: " + e.getMessage();
    }
    LOG.info("*** Successfully executed query");
    return ("*** Successfully executed query");
  }

  @GetMapping(value = "/tradeLoader")
  public String tradeLoader(
          @RequestParam(value = "Command: start/stop", required = true) String command,
          @RequestParam(value = "Number of loops (0 = loop continuously)", required = true)
                  int numLoops,
          @RequestParam(value = "No. to generate", required = true) int num,
          @RequestParam(value = "Msg Interval in millis", required = true) long msgInterval,
          @RequestParam(value = "Batch Interval in millis", required = true) long batchInterval) {
    try {
      if ("START".equalsIgnoreCase(command)) {
        startTradeGenerator.set(true);
        tradeGenerator(numLoops, num, msgInterval, batchInterval);
      } else if ("STOP".equalsIgnoreCase(command)) {
        startTradeGenerator.set(false);
      }
    } catch (Exception e) {
      return "*** Encountered error in method tradeLoader: " + e.getMessage();
    }
    LOG.info("*** Successfully executed query");
    return ("*** Successfully executed query");
  }

  public void tradeGenerator(
          int numLoops, int numToGenerate, long msgInterval, long batchInterval) {

    try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(tradeQueuePath).build();
         ExcerptAppender appender = queue.acquireAppender(); ) {

      TradeHelper tradeHelper = new TradeHelper();

      ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

      numberOfTradeLoops = 0;

      Runnable task =
              () -> generateTradeMessages(numToGenerate, appender, tradeHelper, msgInterval);

      scheduler.scheduleWithFixedDelay(task, 0, batchInterval, TimeUnit.MILLISECONDS);

      while (true) {
        if (!startTradeGenerator.get() || (numLoops > 0 && (numberOfTradeLoops == numLoops))) {
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
    }
  }

  public void quoteGenerator(
          int numLoops, int numToGenerate, long msgInterval, long batchInterval) {

    try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(quoteQueuePath).build();
         ExcerptAppender appender = queue.acquireAppender(); ) {

      QuoteHelper quoteHelper = new QuoteHelper();

      ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

      numberOfQuoteLoops = 0;

      Runnable task =
              () -> generateQuoteMessages(numToGenerate, appender, quoteHelper, msgInterval);

      scheduler.scheduleWithFixedDelay(task, 0, batchInterval, TimeUnit.MILLISECONDS);

      while (true) {
        if (!startQuoteGenerator.get() || (numLoops > 0 && (numberOfQuoteLoops == numLoops))) {
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
    }
  }

  public void generateQuoteMessages(
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
        dc.wire().write("QUOTE").object(quoteHelper.generateQuoteMsg());
      } catch (Exception ex) {
        LOG.error("Error writing message: {}", ex.toString());
      }
      numMsgsWritten++;
    }

    numberOfQuoteLoops++;

    long finish = System.nanoTime() - start;
    long index = appender.lastIndexAppended();

    LOG.info(
            "LOOP: {}; TIMING: Added {} messages (up to index: {}) in {} seconds",
            numberOfQuoteLoops,
            numMsgsWritten,
            index,
            finish / 1e9);
  }

  public void generateTradeMessages(
          long numToGenerate, ExcerptAppender appender, TradeHelper tradeHelper, long msgInterval) {

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
        dc.wire().write("TRADE").object(tradeHelper.generateTradeMsg());
      } catch (Exception ex) {
        LOG.error("Error writing message: {}", ex.toString());
      }
      numMsgsWritten++;
    }

    numberOfTradeLoops++;

    long finish = System.nanoTime() - start;
    long index = appender.lastIndexAppended();

    LOG.info(
            "LOOP: {}; TIMING: Added {} messages (up to index: {}) in {} seconds",
            numberOfTradeLoops,
            numMsgsWritten,
            index,
            finish / 1e9);
  }
}
