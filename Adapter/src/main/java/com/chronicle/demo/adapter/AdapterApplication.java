package com.chronicle.demo.adapter;

import com.chronicle.demo.adapter.kdb.KdbConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import javax.annotation.PreDestroy;

@SpringBootApplication(scanBasePackages = {"com.chronicle.demo.adapter"})
public class AdapterApplication implements CommandLineRunner {

	private static Logger LOG = LoggerFactory.getLogger(AdapterApplication.class);

	@Autowired
	Adapter adapter;

	@Value("${chronicle.source}")
	String chronicleQueueSource;

//	@Autowired
//	KdbConnector kdbConnector;

	public static void main(String[] args) {
		SpringApplication.run(AdapterApplication.class, args);
	}

	@Override
	public void run(String... args) {
		adapter.processMessages(chronicleQueueSource);
	}

	@PreDestroy
	public void onExit(){
		adapter.tidyUp(chronicleQueueSource);
	}

}
