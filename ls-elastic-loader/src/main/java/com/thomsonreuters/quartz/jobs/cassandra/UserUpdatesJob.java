package com.thomsonreuters.quartz.jobs.cassandra;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.PersistJobDataAfterExecution;

import com.datastax.driver.core.ResultSet;
import com.thomsonreuters.util.CassandraUtils;
import com.thomsonreuters.util.ElasticUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.searchbox.client.JestResult;

@PersistJobDataAfterExecution
@DisallowConcurrentExecution
public class UserUpdatesJob implements Job {
  private static final Logger LOG = LoggerFactory.getLogger(UserUpdatesJob.class);
	public static CassandraUtils cassandra; 
	public static ElasticUtils elastic;
	public static boolean firstRun = true;
	

	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		LOG.info("Begin execute...");
		  
		if (firstRun){
		  LOG.info("First loop setup.");
		  firstRun = false;
		  JobDataMap args = context.getJobDetail().getJobDataMap();
		  cassandra = (CassandraUtils) args.get("cassandra");
		  elastic = (ElasticUtils) args.get("elastic");
		}

		ResultSet delta = cassandra.getDelta();
		if (delta.getAvailableWithoutFetching() > 0) {
		  LOG.info("DELTA # -> " + delta.getAvailableWithoutFetching());
			JestResult loadElasticResult = elastic.loadUsersDelta(delta);
			LOG.info("Load Delta Response received...");
			elastic.printJestResult(loadElasticResult);
		}
		//LOG.info("Finished QUARTZ execute loop.");
	}
}
