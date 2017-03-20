package com.thomsonreuters.profile.ingest;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import static org.quartz.CronScheduleBuilder.dailyAtHourAndMinute;

import com.thomsonreuters.quartz.jobs.cassandra.BaselineExportJob;
import com.thomsonreuters.quartz.jobs.cassandra.UserUpdatesJob;
import com.thomsonreuters.util.CassandraUtils;
import com.thomsonreuters.util.ElasticUtils;

import io.searchbox.client.JestResult;

import com.datastax.driver.core.ResultSet;
import com.netflix.config.ConfigurationManager;

public class Ingester {
  private static final Logger LOG = LoggerFactory.getLogger(Ingester.class);
	public static CassandraUtils cassandra; 
	public static ElasticUtils elastic;
	private static String elasticUrl = "";
	private static int elasticPort = 0;
	private static String elasticIndex= "";
	private static String cassandraUrl = "";
	private static int cassandraPort = 0;
	private static String cassandraKeyspace = "";
	private static int deltaWaitTimeSeconds = 30;
	private static boolean loadBaselineOnStart = false;
	private static boolean loadBaselineDaily = false;
	private static int baselineHour = 23;
	private static int baselineMinute = 30;
	
	private static String[] opArgs = null;
	private static Options options;
	static CommandLine cmdArgs;
	private static final String baselineOp = "runBaseline";
	
	public Ingester(CassandraUtils cassandraUtils, ElasticUtils elasticUtils){
	  cassandra = cassandraUtils;
	  elastic = elasticUtils;
	}
	
	Ingester(){
	  init();
	}

	private static void setupOptions() {
		options.addOption(baselineOp, false, "Run embedded test routine.");
	}

	public static void parseOptions(String[] args) {
		CommandLineParser parser = new BasicParser();
		try {
			cmdArgs = parser.parse(options, args);
			if (cmdArgs.hasOption(baselineOp)) {
			  loadBaselineOnStart = true;
			}
		} catch (ParseException e) {
			LOG.error("!!! Error parsing arguments !!!");
			e.printStackTrace();
		}
	}

	public static void runBaseline(){
		LOG.info("Executing Full Baseline Export/Load. Cassandra ---> Elastic");
		ResultSet baseline = cassandra.getBaseline();
		LOG.debug("Baseline from Cassandra received ");
		JestResult loadElasticResult = elastic.loadUsersDelta(baseline);
		LOG.info("Baseline loaded to Elastic!");
		elastic.printJestResult(loadElasticResult);
	}

	public static void dumpProperties(){
	    System.out.println("---------------------------------------");
	    LOG.info("<<< PROPERTIES >>>");
	    LOG.info("elasticUrl : " + elasticUrl);
	    LOG.info("elasticPort : " + elasticPort);
	    LOG.info("elasticIndex : " + elasticIndex);
	    LOG.info("cassandraUrl : " + cassandraUrl);
	    LOG.info("cassandraKeyspace : " + cassandraKeyspace);
	    LOG.info("cassandraPort : " + cassandraPort);
	    LOG.info("waitTimeSeconds : " + deltaWaitTimeSeconds);
	    LOG.info("loadBaslineOnStart : " + loadBaselineOnStart);
	    LOG.info("loadBaslineDaily : " + loadBaselineDaily);
	    LOG.info("baselineHour : " + baselineHour);
	    LOG.info("baselineMinute : " + baselineMinute);
	    System.out.println("---------------------------------------");
	}
	
	private static void init(){
	    LOG.info("Initializing...");
	    options = new Options();
	    setupOptions();
	    try {
        ConfigurationManager.loadCascadedPropertiesFromResources("1p-profile-ingest");
      } catch(IOException e) {
        e.printStackTrace();
      }
	    String blStartupString = ConfigurationManager.getConfigInstance().getString("ingest.baseline.startup.runflag");
	    String blDailyString = ConfigurationManager.getConfigInstance().getString("ingest.baseline.daily.runflag");
	    elasticUrl = ConfigurationManager.getConfigInstance().getString("ingest.elastic.url");
	    elasticPort = ConfigurationManager.getConfigInstance().getInt("ingest.elastic.port");
	    elasticIndex = ConfigurationManager.getConfigInstance().getString("ingest.elastic.index");
	    cassandraUrl = ConfigurationManager.getConfigInstance().getString("ingest.cassandra.url");
	    cassandraKeyspace = ConfigurationManager.getConfigInstance().getString("ingest.cassandra.keyspace");
	    cassandraPort = ConfigurationManager.getConfigInstance().getInt("ingest.cassandra.port");
	    deltaWaitTimeSeconds = ConfigurationManager.getConfigInstance().getInt("ingest.delta.waittime.seconds");
	    baselineHour = ConfigurationManager.getConfigInstance().getInt("ingest.baseline.daily.hour");
	    baselineMinute = ConfigurationManager.getConfigInstance().getInt("ingest.baseline.daily.minute");
	    loadBaselineOnStart = Boolean.parseBoolean(blStartupString);
	    loadBaselineDaily = Boolean.parseBoolean(blDailyString);
		  elastic = new ElasticUtils(elasticUrl, elasticPort, elasticIndex);
		  cassandra = new CassandraUtils(cassandraUrl, cassandraKeyspace);
		  parseOptions(opArgs);
	}

	public static void main(String[] args) throws Exception {
	  opArgs = args;
	  init();
		dumpProperties();
		if (loadBaselineOnStart){
		  runBaseline();
		}
		elastic.shutdown();
		cassandra.shutdown();
	}
}
