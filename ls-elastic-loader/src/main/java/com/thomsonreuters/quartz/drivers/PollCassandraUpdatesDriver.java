package com.thomsonreuters.quartz.drivers;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import com.netflix.config.ConfigurationManager;

public class PollCassandraUpdatesDriver {
  private static final Logger LOG = LoggerFactory.getLogger(PollCassandraUpdatesDriver.class);
	private static Integer runCount = 0;
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
	private static final String DELTA_JOB_NAME = "userDeltaJob";
	private static final String DELTA_TRIGGER_NAME = "userDeltaTrigger";
	private static final String BASELINE_JOB_NAME = "baselineExportJob";
	private static final String BASELINE_STARTUP_JOB_NAME = "baselineStartupExportJob";
	private static final String BASELINE_TRIGGER_NAME = "baselineTrigger";
	private static final String GROUP_NAME = "cassandraToElasicXferGroup";

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
	  if (runCount == 0){
	    LOG.info("First run...");
	    runCount++;
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
	  }
	}

	public static void main(String[] args) throws Exception {

	  init();
		dumpProperties();
	  LOG.info("Creating Quartz jobs...");
		JobKey baselineStartupKey = JobKey.jobKey(BASELINE_STARTUP_JOB_NAME, GROUP_NAME);
		JobDetail deltaJob = JobBuilder.newJob(UserUpdatesJob.class).withIdentity(DELTA_JOB_NAME, GROUP_NAME).build();
		JobDetail baselineJob = JobBuilder.newJob(BaselineExportJob.class).withIdentity(BASELINE_JOB_NAME, GROUP_NAME).build();
		JobDetail baselineStartupJob = JobBuilder.newJob(BaselineExportJob.class).withIdentity(baselineStartupKey).storeDurably().build();
		
	  LOG.info("Creating elastic utils...");
		ElasticUtils elastic = new ElasticUtils(elasticUrl, elasticPort, elasticIndex);
	  LOG.info("Creating cassandra utils...");
		CassandraUtils cassandra = new CassandraUtils(cassandraUrl, cassandraKeyspace);

	  LOG.info("Assigning data to Quartz jobs...");
		baselineJob.getJobDataMap().put("loadBaselineOnStart", loadBaselineOnStart);
	  LOG.info("Assigning utils to Quartz jobs...");
		deltaJob.getJobDataMap().put("elastic", elastic);
		deltaJob.getJobDataMap().put("cassandra", cassandra);
		baselineJob.getJobDataMap().put("elastic", elastic);
		baselineJob.getJobDataMap().put("cassandra", cassandra);

	  LOG.info("Creating quartz triggers ...");
		Trigger deltaTrigger = TriggerBuilder.newTrigger().withIdentity(DELTA_TRIGGER_NAME, GROUP_NAME)
				.withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(deltaWaitTimeSeconds).repeatForever())
				.build();

		Trigger baselineTrigger = TriggerBuilder.newTrigger().withIdentity(BASELINE_TRIGGER_NAME, GROUP_NAME)
				.withSchedule(dailyAtHourAndMinute(baselineHour, baselineMinute))
				.startNow()
				.build();

	  LOG.info("Creating quartz scheduler ...");
		Scheduler scheduler = new StdSchedulerFactory().getScheduler();
	  LOG.info("Starting scheduler ...");
		scheduler.start();
	  LOG.info("Scheduling jobs...");
		if (loadBaselineOnStart){
		  LOG.info("loadBaselineOnStart = TRUE");
		  scheduler.addJob(baselineStartupJob,true);
		  scheduler.triggerJob(baselineStartupKey);
		}
		scheduler.scheduleJob(deltaJob, deltaTrigger);
		scheduler.scheduleJob(baselineJob, baselineTrigger);
	  LOG.info("Quartz jobs scheduled and running...");
	}
}
