package com.thomsonreuters.util;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.Math;
import java.util.UUID;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

public class CassandraUtils {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraUtils.class);
	private static Integer initializeCt = 0;
	private static ArrayList<String> truidList;
	private static Random random;
	private static int MAX_RECS = 5;
	private static int INTERVAL = 1;
	private static String CONNECT_URL = "127.0.0.1";
	private static String KEYSPACE = "profiles";
	private static int INSERT_TIMEOUT = 20000;
	private static final String SEP = "---------------------------------------";
	private static Cluster cluster;
	private static Session session;
	private static Options options;
	private static String[] opArgs = null;
	static CommandLine cmdArgs;
	private static PreparedStatement deltaTruidPreparedStatement; 
	private static PreparedStatement deltaListInsertPreparedStatement; 
	private static PreparedStatement userDeltaPreparedStatement;
	private static PreparedStatement createBookmarkPreparedStatement;
	private static PreparedStatement getBookmarkPreparedStatement;
	private static PreparedStatement updateBookmarkPreparedStatement;

	
	private static String deltaListPath;
	private static final String insertTestOp = "insertTest";
	private static final String dumpOp = "dumpOptions";
	private static final String deltaListOp = "deltaList";
	private static final String insertNumOp = "insertNum";
	private static final String insertIntervalOp = "insertInterval";
	private static final String urlOp= "connectUrl";
	private static final String keyspaceOp = "keyspace";

	private static final String truidQuery = "select truid from profiles.users limit 3000";
	private static final String userDeltaQuery = "select * from profiles.users where truid in ?";
	private static final String userBaselineQuery = "select * from profiles.users";
	private static final String getBookmarkQuery = "select bookmark_date from profiles.delta_bookmark where bookmark_name = 'users_delta'";
	private static final String createBookmarkQuery = "INSERT INTO profiles.delta_bookmark (bookmark_name, bookmark_date) VALUES('users_delta', ?)";
	private static final String updateBookmarkQuery = "UPDATE profiles.delta_bookmark SET bookmark_date = ? where bookmark_name = 'users_delta'";
	private static final String userInsert_1 = "INSERT INTO users (truid, hcrid, first_name, middle_name, last_name, title, role, primary_institution, location, interest, category, hcr_indicator, ut,summary) VALUES(";
  private static final String userInsert_2 = ",5,'Dale','E','Bauman','','Mage','Cornell University','USA',{'Synthesizers', 'Computers'},'Agricultural Sciences',TRUE,{'439516768124864', '656513519643664', '762221165926713'},'This is my summary')";
	private static final String deltaSinceBookmarkQuery = "select * from profiles.users_delta where event_date > ? ALLOW FILTERING";
	private static final String deltaInsertQuery = "INSERT INTO profiles.users_delta (truid, event_date) SET truid = ?,  event_date > ?";

	public CassandraUtils() {
		init("127.0.0.1", "profiles");
	}

	public CassandraUtils(String url, String keyspace) {
		init(url, keyspace);
	}

	private static Boolean isInitialized() {
		return initializeCt > 0;
	}

	public static void init(String connectURL, String keyspace) {
		if (!isInitialized()) {
		  options = new Options();
		  setupOptions();
		  parseOptions(opArgs);
			truidList = new ArrayList<String>();
			cluster = Cluster.builder().addContactPoint(connectURL).build();
			session = cluster.connect(keyspace);
//			deltaListInsertPreparedStatement = session.prepare(deltaInsertQuery);
			deltaTruidPreparedStatement = session.prepare(deltaSinceBookmarkQuery);
		  userDeltaPreparedStatement = session.prepare(userDeltaQuery);
		  createBookmarkPreparedStatement = session.prepare(createBookmarkQuery);
		  updateBookmarkPreparedStatement= session.prepare(updateBookmarkQuery);
		  getBookmarkPreparedStatement = session.prepare(getBookmarkQuery);
			initializeCt++;
			random = new Random(System.currentTimeMillis());
			populateDeltaTruids();
		}
	}

	private static void setupOptions() {
		options.addOption("t", insertTestOp, false, "Run insert stress test.");
		options.addOption("d", dumpOp, false, "Print options to screen");
		options.addOption(deltaListOp, true, "File containing list of truids to trigger delta ingestion.");
		options.addOption(insertNumOp, true, "Number of mock users to insert.");
		options.addOption(insertIntervalOp, true, "Time (miliseconds) between insert of mock users.");
		options.addOption(urlOp, true, "Cassandra Connection URL.");
		options.addOption(keyspaceOp, true, "Cassandra keyspace to use.");
	}

	public static void parseOptions(String[] args) {
		CommandLineParser parser = new BasicParser();
		try {
			cmdArgs = parser.parse(options, args);
			if (cmdArgs.hasOption(insertNumOp)) {
			  MAX_RECS = Integer.parseInt(cmdArgs.getOptionValue(insertNumOp));
			}
			if (cmdArgs.hasOption(insertIntervalOp)) {
			  INTERVAL = Integer.parseInt(cmdArgs.getOptionValue(insertIntervalOp));
			}
			if (cmdArgs.hasOption(urlOp)) {
			  CONNECT_URL = cmdArgs.getOptionValue(urlOp);
			}
			if (cmdArgs.hasOption(keyspaceOp)) {
			  KEYSPACE = cmdArgs.getOptionValue(keyspaceOp);
			}
			if (cmdArgs.hasOption(deltaListOp)) {
			  deltaListPath = cmdArgs.getOptionValue(deltaListOp);
			}

		} catch (ParseException e) {
			LOG.error("!!! Error parsing arguments !!!");
			e.printStackTrace();
		}
	}

	public static void shutdown() {
		session.close();
		cluster.close();
	}

	public static String generateTruid() {
		return UUID.randomUUID().toString();
	}

	// Pull info from cassandra and populate a local array with valid TRUIDs
	public static void populateDeltaTruids() {
		ResultSet results = execute(truidQuery);
		Iterator<Row> iter = results.iterator();
		while (iter.hasNext()) {
			Row row = iter.next();
			truidList.add(row.getString("truid"));
		}
	}

	public static String getRandomTruid() {
		return truidList.get(Math.abs(random.nextInt()) % truidList.size());
	}

	public static void insertMockRecord(String truid) {
		Date insertDate = new java.util.Date();
		String statement = userInsert_1 + "'" + truid + "'" + userInsert_2;
		execute(statement);
	}

	public static void insertDeltaUpdate(String truid) {
		Date insertDate = new java.util.Date();
		String statement = "INSERT INTO users_delta (truid, event_date) VALUES('" + truid + "','"
				+ insertDate.getTime() + "');";
		execute(statement);
	}
	
	private static List<String> readDeltaList(String filepath){
	  List<String> deltaList = null;
	  try {
      deltaList = FileUtils.readLines(new File(filepath), "utf-8");
    } catch(IOException e) {
      LOG.error("Problem Reading " + filepath);
      e.printStackTrace();
    }
	  return deltaList;
	}
	
	//insert a list of truids into the delta table
	public static void insertDeltaList(List<String> deltaList){
	  for (String truid : deltaList){
			LOG.debug("Inserting (list) -> " + truid);
	    insertDeltaUpdate(truid);
	  }
	}

	// Randomly insert records into users_delta table
	public static void insertStressTest(Integer insertInterval, Integer maxRecs, Integer timeoutInSeconds) {
		int recsInserted = 0;
		try {
			while (recsInserted < maxRecs) {
				Thread.sleep(insertInterval);
				String truid = java.util.UUID.randomUUID().toString();
				LOG.debug("Inserting -> " + truid);
		//Follow api behavior, insert into both users and users_delta tables
				insertMockRecord(truid);
				insertDeltaUpdate(truid);
				recsInserted++;
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
			LOG.error("ERROR loading mock records. Exiting.");
			shutdown();
			System.exit(1);
		}
	}

	public static Date createBookmark() {
		Date bookmarkDate = new java.util.Date();
		BoundStatement setBookmarkStatement = new BoundStatement(createBookmarkPreparedStatement);
		setBookmarkStatement.bind(bookmarkDate);
		session.execute(setBookmarkStatement);
		return bookmarkDate;
	}

	public static Date updateBookmark() {
		Date bookmarkDate = new java.util.Date();
		BoundStatement updateBookmarkStatement = new BoundStatement(updateBookmarkPreparedStatement);
		updateBookmarkStatement.bind(bookmarkDate);
		session.execute(updateBookmarkStatement);
		return bookmarkDate;
	}

	// get bookmark from delta_bookmark table
	public static Date getBookmark() {
	  Date dbBookmark;
		BoundStatement bookmarkStatement = new BoundStatement(getBookmarkPreparedStatement);
		ResultSet bookmarkResults = session.execute(bookmarkStatement);
		if (bookmarkResults.getAvailableWithoutFetching() > 0){
		  dbBookmark = bookmarkResults.one().getDate("bookmark_date");
		  LOG.debug("BOOKMARK FOUND : " + dbBookmark.toString());
		}else{
		  // No Bookmark Available for 'users_delta' lable. Create one.
		  dbBookmark = createBookmark(); 
		  LOG.debug("BOOMARK NOT FOUND -> creating bookmark set to " + dbBookmark.toString());
		}
		return dbBookmark;
	}

	// get the list of truids that have been changed since the last bookmark.
	public static ArrayList<String> getDeltaTruids() {
		Date bookmarkDate = getBookmark();
		BoundStatement deltaTruidStatement = new BoundStatement(deltaTruidPreparedStatement);
		deltaTruidStatement.bind(bookmarkDate);
		ResultSet deltaTruidResults = session.execute(deltaTruidStatement);
		Iterator<Row> iter = deltaTruidResults.iterator();
		ArrayList<String> deltaTruidList = new ArrayList<String>();
		while (iter.hasNext()) {
			Row row = iter.next();
			deltaTruidList.add(row.getString("truid"));
		}
		return deltaTruidList;
	}

	// Get all user records updated since the last bookmark and reset the
	// bookmark
	public static ResultSet getDelta() {
		ArrayList<String> deltaTruids = getDeltaTruids();
		BoundStatement userStatement = new BoundStatement(userDeltaPreparedStatement);
		userStatement.bind(deltaTruids);
		ResultSet userDelta = session.execute(userStatement);
		if (userDelta.getAvailableWithoutFetching() > 0){
		  updateBookmark(); // reset bookmark so next batch contains only new updates
		}
		return userDelta;
	}

	public static ResultSet getBaseline() {
		PreparedStatement baselinePreparedStatment = session.prepare(userBaselineQuery);
		BoundStatement userStatement = new BoundStatement(baselinePreparedStatment);
		ResultSet userData = session.execute(userStatement);
		return userData;
	}

	public static ResultSet execute(String statement) {
		return session.execute(statement);
	}

	public static void printResults(ResultSet rs) {
		System.out.println(SEP);
		for (Row row : rs) {
			System.out.println(row.toString());
		}
		System.out.println(SEP);
	}

	private static void dumpOptions() {
		System.out.println(SEP);
		LOG.info("<<< OPTION VALUES >>>");
		for (Option op : cmdArgs.getOptions()) {
			LOG.info(op.toString() + " --> " + op.getValue());
		}
		System.out.println(SEP);
	}

	public static void main(String[] args) {
		opArgs = args;
		init(CONNECT_URL, KEYSPACE);
		if (cmdArgs.hasOption(dumpOp)) {
			dumpOptions();
		}
		if (cmdArgs.hasOption(insertTestOp)){
		  insertStressTest(INTERVAL, MAX_RECS, INSERT_TIMEOUT);
		}
		if (cmdArgs.hasOption(deltaListOp)){
		  List<String> truidList = readDeltaList(deltaListPath);
		  insertDeltaList(truidList);
		}
		shutdown();
	}
}
