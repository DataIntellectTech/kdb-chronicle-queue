package com.kdb.adapter.bench;

import com.kdb.adapter.chronicle.ChronicleToKdbAdapter;
import com.kdb.adapter.data.QuoteHelper;
import com.kdb.adapter.messages.ChronicleQuoteMsg;

import com.kdb.adapter.utils.AdapterProperties;
import com.kdb.adapter.utils.PropertyFileLoader;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.jlbh.JLBH;
import net.openhft.chronicle.core.jlbh.JLBHOptions;
import net.openhft.chronicle.core.jlbh.JLBHTask;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;

public class AdapterBenchmark implements JLBHTask {

  private SingleChronicleQueue sourceQueue;
  private ExcerptAppender appender;
  QuoteHelper quoteHelper = new QuoteHelper();
  private static final Logger LOG = LoggerFactory.getLogger(AdapterBenchmark.class);

  public static void main(String[] args) {
    JLBHOptions lth =
        new JLBHOptions()
            .warmUpIterations(50_000)
            .iterations(300_000)
            .throughput(30_000)
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
    try {

      // Check application.properties for runMode...
      // adapter.runMode=BENCH -> benchmarking mode, generates its own messages and writes to kdb+

      final PropertyFileLoader properties = new PropertyFileLoader();
      final Properties props = properties.getPropValues("application.properties");
      final AdapterProperties adapterProperties = new AdapterProperties(props);
      final String queueName = adapterProperties.getChronicleSource();
      final ChronicleToKdbAdapter adapter =
          new ChronicleToKdbAdapter(
              adapterProperties.getAdapterMessageType(), adapterProperties, jlbh);

      IOTools.deleteDirWithFiles(queueName, 10);

      sourceQueue = single(queueName).build();
      appender = sourceQueue.acquireAppender();

      Thread thread = new Thread(adapter);
      thread.start();

    } catch (Exception ex) {
      LOG.error("Error: {}", ex.toString());
    }
  }

  private SingleChronicleQueueBuilder single(String queueName) {
    return ChronicleQueue.singleBuilder(queueName).readOnly(false).testBlockSize();
  }

  @Override
  public void run(long startTimeNS) {
    ChronicleQuoteMsg writeQuote = quoteHelper.generateQuoteMsg();
    writeQuote.ts = startTimeNS;
    try (DocumentContext dc = appender.writingDocument()) {
      dc.wire().write("QUOTE").object(writeQuote);
    }
  }

  @Override
  public void complete() {
    sourceQueue.close();
    System.exit(0);
  }
}
