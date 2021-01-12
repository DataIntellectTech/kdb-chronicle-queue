package com.kdb.adapter;

import com.kdb.adapter.chronicle.ChronicleKdbAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.PreDestroy;

@SpringBootApplication(scanBasePackages = {"com.kdb.adapter"})
public class AdapterApplication implements CommandLineRunner {

	private static Logger LOG = LoggerFactory.getLogger(AdapterApplication.class);

	@Autowired
	ChronicleKdbAdapter adapter;

	public static void main(String[] args) {
		SpringApplication.run(AdapterApplication.class, args);
	}

	@Override
	public void run(String... args) {
		adapter.processMessages();
	}

	@PreDestroy
	public void onExit(){
		adapter.tidyUp();
	}

}