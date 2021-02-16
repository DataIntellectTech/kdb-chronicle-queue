package com.kdb.adapter.bench;

import com.kdb.adapter.data.QuoteHelper;
import com.kdb.adapter.messages.ChronicleQuoteMsg;

import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.jlbh.JLBH;
import net.openhft.chronicle.core.jlbh.JLBHOptions;
import net.openhft.chronicle.core.jlbh.JLBHTask;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.single;

import java.time.LocalDateTime;

public class AdapterBenchmark implements JLBHTask {

  private SingleChronicleQueue sourceQueue;
  private SingleChronicleQueue sinkQueue;
  private ExcerptAppender appender;
  QuoteHelper quoteHelper = new QuoteHelper();

  public static void main(String[] args) {
    JLBHOptions lth =
        new JLBHOptions()
            .warmUpIterations(50000)
            .iterations(1_000_000)
            .throughput(100_000)
            .recordOSJitter(false)
            // disable as otherwise single GC event skews results heavily
            .accountForCoordinatedOmmission(false)
            .skipFirstRun(true)
            .runs(5)
            .jlbhTask(new AdapterBenchmark());
    new JLBH(lth).start();
  }

  @Override
  public void init(JLBH jlbh) {

    final String queueName="replica";

    IOTools.deleteDirWithFiles(queueName, 10);

    sourceQueue = single(queueName).build();
    sinkQueue = single(queueName).build();
    appender = sourceQueue.acquireAppender();
    ExcerptTailer tailer = sinkQueue.createTailer();

    new Thread(
            () -> {
              while (true) {
                try (DocumentContext dc = tailer.readingDocument()) {
                  if (dc.wire() == null) continue;

                  ChronicleQuoteMsg readQuote = (ChronicleQuoteMsg)dc.wire().read("Quote").object();

                  // Could do kdb part here

                  jlbh.sample(System.nanoTime() - readQuote.ts);
                }
              }
            })
        .start();
  }

  private SingleChronicleQueueBuilder single(String queueName) {
    return ChronicleQueue.singleBuilder(queueName)
            .readOnly(false)
            .testBlockSize();
  }

  @Override
  public void run(long startTimeNS) {
    ChronicleQuoteMsg writeQuote = quoteHelper.generateQuoteMsg();
    writeQuote.ts = startTimeNS;
    try (DocumentContext dc = appender.writingDocument()) {
      dc.wire().write("Quote").object(writeQuote);
    }
  }

  @Override
  public void complete() {
    sinkQueue.close();
    sourceQueue.close();
    System.exit(0);
  }
}
