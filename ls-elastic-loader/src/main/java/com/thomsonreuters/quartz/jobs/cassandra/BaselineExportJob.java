package com.thomsonreuters.quartz.jobs.cassandra;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.PersistJobDataAfterExecution;

import com.thomsonreuters.profile.ingest.Ingester;
import com.thomsonreuters.util.CassandraUtils;
import com.thomsonreuters.util.ElasticUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@PersistJobDataAfterExecution
@DisallowConcurrentExecution
public class BaselineExportJob implements Job {
  private static final Logger LOG = LoggerFactory.getLogger(BaselineExportJob.class);
	private static Ingester ingester;
	public static boolean firstRun = true;
	
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
	  LOG.info("Begin Baseline export loop...");
		if (firstRun){
		  LOG.info("First loop setup.");
		  firstRun = false;
		  JobDataMap args = context.getJobDetail().getJobDataMap();
		  ingester = new Ingester ((CassandraUtils) args.get("cassandra"),
		                           (ElasticUtils) args.get("elastic"));
		}
		LOG.info("Exporting basline from cassandra to elastic...");
		ingester.runBaseline();
		LOG.info("Finished BASELINE QUARTZ execute loop.");
	}
}
