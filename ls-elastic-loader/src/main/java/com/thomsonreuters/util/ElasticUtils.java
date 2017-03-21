package com.thomsonreuters.util;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.io.InputStream;
import java.io.InputStreamReader;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Bulk.Builder;
import io.searchbox.core.Delete;
import io.searchbox.core.Index;
import io.searchbox.core.Search;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.mapping.PutMapping;
import io.searchbox.params.Parameters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.gson.JsonObject;
import com.netflix.governator.annotations.Configuration;

public class ElasticUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ElasticUtils.class);

  private static final String defaultCountryDataJson = "json/CountryData.json";
  private static final String defaultOrgDataJson = "json/OrganizationData.json";
  private static final String defaultSettingsJson = "json/elastic_autocomplete_settings.json";
  private static final String defaultUserMappingJson = "json/elastic_asciifolding_w_ngram_user_mapping.json";
  private static final String defaultUserDataJson = "json/elastic_users_mock_data.json";
  private static final String defaultCountryMappingJson = "json/elastic_country_mapping.json";
  private static final String defaultOrgMappingJson = "json/elastic_organization_mapping.json";

	@Configuration("listsales.elastic.url")
	  public static String elasticUrl = "localhost";

  private static final String defaultUserDataPath = 
        "./src/main/resources/json/elastic_users_mock_data.json";

	public static String userDataPath;
	private static String[] opArgs = null;

	private static final String settingsOp = "settings";
	private static final String makeAllOp = "makeAll";
	private static final String makeAllEmpty = "makeAllEmpty";
	private static final String makeUser = "makeUser";
	private static final String makeUserEmpty = "makeUserEmpty";
	private static final String makeCountry = "makeCountry";
	private static final String makeCountryEmpty = "makeCountryEmpty";
	private static final String makeOrg = "makeOrg";
	private static final String makeOrgEmpty = "makeOrgEmpty";
	private static final String testOp = "test";
	private static final String dumpOp = "dumpOptions";
	private static final String userMappingOp = "userMapping";
	private static final String userDataOp = "userData";
	private static final String countryMappingOp = "countryMapping";
	private static final String countryDataOp = "countryData";
	private static final String orgDataOp = "orgData";
	private static final String orgMappingOp = "orgMapping";
	private static final String portOp = "port";
	private static final String hostOp = "host";
	private static final String indexOp = "index";
	private static final String clusterOp = "cluster";

	private static final String SEP = "---------------------------------------";
	private static final int TIMEOUT = 3000;
	private static String elasticindex = "profile";
	private static String CLUSTER = "elasticsearch";
	private static final String USER_TYPE = "user";
	private static final String ORG_TYPE = "organization";
	private static final String COUNTRY_TYPE = "country";
	private static final int REMOTE_PORT = 9200;
	private static JestClient jclient;
	public static JSONObject settingsJson;
	public static JSONObject userMapping;
	public static JSONObject countryMapping;
	public static JSONObject orgMapping;
	public static JSONObject countryData;
	public static JSONObject orgData;
	public static JSONArray countries;
	public static JSONArray organizations;
	private static Options options;

	static CommandLine cmdArgs;

	public ElasticUtils() {
		init("http://localhost", 9200, elasticindex);
	}

	public ElasticUtils(String url, int port) {
		this(url, port, null);
	}
	
	public ElasticUtils(String url, int port, String index) {
		init(url, port, index);
	}
	
	private static void setupOptions() {
		options.addOption("t", testOp, false, "Run embedded test routine.");
		options.addOption("d", dumpOp, false, "Show options and values");
		options.addOption("m", makeAllOp, false, "Create all indices and types, load with data");
		options.addOption("e", makeAllEmpty, false, "Create all indices and types, keep empty.");
		options.addOption(makeUser, false, "Create and populate User type");
		options.addOption(makeUserEmpty, false, "Create User type only. No data load.");
		options.addOption(makeCountry, false, "Create and populate Country type");
		options.addOption(makeCountryEmpty, false, "Create Country type only. No data load.");
		options.addOption(makeOrg, false, "Create and populate Organization type");
		options.addOption(makeOrgEmpty, false, "Create Organization type only. No data load.");
		options.addOption("s", settingsOp, true, "Custom settings to apply when creating the index.");
		options.addOption("u", userDataOp, true, "User data to load. JSON format.");
		options.addOption("U", userMappingOp, true, "Custom mappings for user data. JSON format.");
		options.addOption("c", countryDataOp, true, "Country data to load. JSON format.");
		options.addOption("C", countryMappingOp, true, "Custom mappings for country data. JSON format.");
		options.addOption("o", orgDataOp, true, "Organization data to load. JSON format.");
		options.addOption("O", orgMappingOp, true, "Custom mappings for organization data. JSON format.");
		options.addOption("h", hostOp, true, "Host URL to connect.");
		options.addOption("p", portOp, true, "Port number to connect.");
		options.addOption("i", indexOp, true, "Index name to use.");
		options.addOption(clusterOp, true, "Cluster to connect to");
	}

	private static void dumpOptions() {
		System.out.println(SEP);
		LOG.info("<<< OPTION VALUES >>>");
		for (Option op : cmdArgs.getOptions()) {
			LOG.info(op.toString() + " --> " + op.getValue());
		}
		System.out.println(SEP);
	}

	public static void parseOptions(String[] args) {
		CommandLineParser parser = new BasicParser();
		try {
			cmdArgs = parser.parse(options, args);
			if (cmdArgs.hasOption(indexOp)) {
				elasticindex = cmdArgs.getOptionValue(indexOp);
			}
			if (cmdArgs.hasOption(clusterOp)) {
				CLUSTER = cmdArgs.getOptionValue(clusterOp);
			}
			if (cmdArgs.hasOption(settingsOp)) {
				settingsJson = readJsonFromFilepath(new File("").getAbsolutePath() + cmdArgs.getOptionValue(settingsOp));
			}
			if (cmdArgs.hasOption(userMappingOp)) {
				userMapping = readJsonFromFilepath(new File("").getAbsolutePath() + cmdArgs.getOptionValue(userMappingOp));
			}
			if (cmdArgs.hasOption(userDataOp)) {
				userDataPath = new File("").getAbsolutePath() + cmdArgs.getOptionValue(userDataOp);
			}
			if (cmdArgs.hasOption(countryMappingOp)) {
				countryMapping = readJsonFromFilepath(new File("").getAbsolutePath()
						+ cmdArgs.getOptionValue(countryMappingOp));
			}
			if (cmdArgs.hasOption(countryDataOp)) {
				countryData = readJsonFromFilepath(new File("").getAbsolutePath() + cmdArgs.getOptionValue(countryDataOp));
			}
			if (cmdArgs.hasOption(orgMappingOp)) {
				orgMapping = readJsonFromFilepath(new File("").getAbsolutePath() + cmdArgs.getOptionValue(orgMappingOp));
			}
			if (cmdArgs.hasOption(orgDataOp)) {
				orgData = readJsonFromFilepath(new File("").getAbsolutePath() + cmdArgs.getOptionValue(orgDataOp));
			}
		} catch (ParseException e) {
			LOG.error("!!! Error parsing arguments !!!");
			e.printStackTrace();
		}
	}

	private static void init(String url, int port, String index) {
		options = new Options();
		setupOptions();
		if(index != null) elasticindex = index;
		userDataPath = defaultUserDataPath;

		settingsJson = readJsonResource(defaultSettingsJson);
		userMapping = readJsonResource(defaultUserMappingJson);
		countryMapping = readJsonResource(defaultCountryMappingJson);
		countryData = readJsonResource(defaultCountryDataJson);
		orgMapping = readJsonResource(defaultOrgMappingJson);
		orgData = readJsonResource(defaultOrgDataJson);

		parseOptions(opArgs);
		countries = (JSONArray) countryData.get("countries");
		organizations = (JSONArray) orgData.get("organizations");
		if (cmdArgs.hasOption(hostOp) && cmdArgs.hasOption(portOp)) {
			initJest(cmdArgs.getOptionValue(hostOp), Integer.parseInt(cmdArgs.getOptionValue(portOp)));
		} else {
			initJest(url, port);
		}
	}

	public static JSONArray readJsonArray(String filepath) {
		FileReader reader = null;
		JSONParser jsonParser = null;
		JSONArray returnArray = null;
		try {
			reader = new FileReader(filepath);
			jsonParser = new JSONParser();
			returnArray = (JSONArray) jsonParser.parse(reader);
		} catch (IOException e) {
			e.printStackTrace();
		} catch(org.json.simple.parser.ParseException pe) {
      pe.printStackTrace();
    }
		return returnArray;
	}

	public static JSONObject readJsonResource(String configName) {
		JSONParser jsonParser = null;
		JSONObject json = null;
		try {
		  InputStream inputStream = ElasticUtils.class.getClassLoader().getResourceAsStream(configName);
		  InputStreamReader inStreamReader = new InputStreamReader(inputStream);
			jsonParser = new JSONParser();
			json = (JSONObject) jsonParser.parse(inStreamReader);
		} catch (IOException | org.json.simple.parser.ParseException e) {
			e.printStackTrace();
		}
		return json;
	}

	public static JSONObject readJsonFromFilepath(String filepath) {
		FileReader reader = null;
		JSONParser jsonParser = null;
		JSONObject json = null;
		try {
			reader = new FileReader(filepath);
			jsonParser = new JSONParser();
			json = (JSONObject) jsonParser.parse(reader);
		} catch (IOException | org.json.simple.parser.ParseException e) {
			e.printStackTrace();
		}
		return json;
	}

	public static JestResult createIndex(JSONObject settings, String indexName) {
		JestResult result = null;
		try {
			result = jclient.execute(new CreateIndex.Builder(indexName).settings(
					ImmutableSettings.builder().loadFromSource(settings.toJSONString()).build().getAsMap()).build());
			printJestResult(result);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}

	public static JestResult createType(JSONObject mapping, String indexName, String typeName) {
		JestResult result = null;
		PutMapping putMapping = new PutMapping.Builder(indexName, typeName, mapping).build();
		try {
			result = jclient.execute(putMapping);
			printJestResult(result);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}

	public static void deleteIndex(String idx) {
		try {
			JestResult response = jclient.execute(new Delete.Builder("1").index(idx).build());
			LOG.info(response.getJsonString());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static XContentBuilder createUserJson(JSONObject user) {
		XContentBuilder json = null;
		try {
			json = XContentFactory.jsonBuilder().startObject().field("firstName", user.get("firstName"))
					.field("middleName", user.get("middleName")).field("lastName", user.get("lastName"))
					.field("primaryInstitution", user.get("primaryInstitution"))
					.field("location", user.get("location")).field("role", user.get("role"))
					.field("interest", user.get("interest")).endObject();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return json;
	}

	public static XContentBuilder createCountryJson(JSONObject country) {
		XContentBuilder json = null;
		try {
			json =

			XContentFactory.jsonBuilder().startObject().field("name", country.get("firstName"))
					.field("id", country.get("id")).endObject();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return json;
	}

	public static XContentBuilder createOrganizationJson(JSONObject org) {
		XContentBuilder json = null;
		try {
			json =

			XContentFactory.jsonBuilder().startObject().field("preferredName", org.get("firstName"))
					.field("parentId", org.get("parent-id")).field("id", org.get("id")).endObject();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return json;
	}

	public static JestResult loadType(JSONArray jsonArray, String typeToLoad) {
		JestResult result = null;
		Builder bulkBuilder = new Bulk.Builder();
		Iterator<JSONObject> iter = jsonArray.iterator();
		while (iter.hasNext()) {
			JSONObject jsonToLoad = (JSONObject) iter.next();
			bulkBuilder.addAction(new Index.Builder(jsonToLoad.toJSONString()).index(elasticindex).type(typeToLoad)
					.setParameter(Parameters.REFRESH, true).build());
		}
		Bulk bulk = bulkBuilder.build();
		try {
			result = jclient.execute(bulk);
			// printJestResult(result);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}

	public static XContentBuilder cassandraRowToElastic(Row row) {
		XContentBuilder json = null;
		try {
		  String firstName = row.getString("first_name");
		  String middleName = row.getString("middle_name");
		  String lastName = row.getString("last_name");
		  String title = row.getString("title");
		  if (StringUtils.isEmpty(title)) {
			title = "";
			if(StringUtils.isNotEmpty(firstName)) {
				title = title + firstName + " ";
			}
			if(StringUtils.isNotEmpty(middleName)) {
				title = title + middleName + " ";
			}
			if(StringUtils.isNotEmpty(lastName)) {
				title = title + lastName;
			}
			title = title.trim();
		  }
			json = XContentFactory.jsonBuilder().startObject()
			    .field("firstName", row.getString("first_name"))
					.field("middleName", row.getString("middle_name"))
					.field("lastName", row.getString("last_name"))
					.field("primaryInstitution", row.getString("primary_institution"))
					.field("location", row.getString("location"))
					.field("role", row.getString("role"))
					.field("interest", row.getSet("interest", java.lang.String.class))
					.field("title", title)  // standard title
					.field("titleNgram", title) // title processed with edge ngram filter for search
					.endObject();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return json;
	}

	public static JestResult loadUsersDelta(ResultSet rs) {
		LOG.debug("Delta -> " + rs.getAvailableWithoutFetching());
		int loadCount = 0;
		JestResult result = null;
		try {
			Iterator<Row> iter = rs.iterator();
			Builder bulkBuilder = new Bulk.Builder();
			while (iter.hasNext()) {
			  loadCount++;
			  if (rs.getAvailableWithoutFetching() < 100 && !rs.isExhausted()){
			    //LOG.debug("Only " + rs.getAvailableWithoutFetching() + " rows in result set. Fetching more results");
			    rs.fetchMoreResults();
			  }
				Row row = iter.next();
				String truid = row.getString("truid");
				XContentBuilder deltaRow = cassandraRowToElastic(row);
				bulkBuilder.addAction(new Index.Builder(deltaRow.string()).index(elasticindex).type(USER_TYPE).id(truid)
						.setParameter(Parameters.REFRESH, true).build());
			}
			Bulk bulk = bulkBuilder.build();
			result = jclient.execute(bulk);
		} catch (IOException e) {
			e.printStackTrace();
		}
		LOG.debug(loadCount + " Rows Loaded to elastic.");
		return result;
	}

	public static JestResult loadUsersBulk(String dataPath) {
		JestResult result = null;
		try {
			Builder bulkBuilder = new Bulk.Builder();
			JSONArray users = readJsonArray(dataPath);
			Iterator<JSONObject> usersIterator = users.iterator();
			String userId = "";
			while (usersIterator.hasNext()) {
				JSONObject user = (JSONObject) usersIterator.next();
				userId = (String) user.get("id");
				XContentBuilder user_json = createUserJson(user);
				bulkBuilder.addAction(new Index.Builder(user_json.string()).index(elasticindex).type(USER_TYPE).id(userId)
						.setParameter(Parameters.REFRESH, true).build());
			}
			Bulk bulk = bulkBuilder.build();
			result = jclient.execute(bulk);
			//printJestResult(result);
		} catch (IOException e) {
			LOG.error("ERROR LOADING USERS");
			e.printStackTrace();
		}
		return result;
	}

	public static void loadSingleMockUser() {
		XContentBuilder user_json = null;
		try {
			user_json = XContentFactory.jsonBuilder().startObject().field("firstName", "Jimmy")
					.field("middleName", "F").field("lastName", "Hendrix").field("primaryInstitution", "RRHOF")
					.field("location", "electricladyland").field("role", "Guitar God")
					.field("interest", "[Sex, drugs, rock and roll]").endObject();
			Index index = new Index.Builder(user_json.string()).index(elasticindex).type(USER_TYPE).build();
			jclient.execute(index);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void shutdown() {
		jclient.shutdownClient();
		System.exit(0);
	}

	public static void remoteShutdown() {
		jclient.shutdownClient();
	}

	public static void printJestResult(JestResult result) {
		//LOG.info(SEP);
		LOG.debug("JEST Result -> " + result.getJsonString());
		//LOG.info(SEP);
	}

	public static JestResult search(String typeToQuery, String field, String searchTerm) {
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.query(QueryBuilders.matchQuery(field, searchTerm));

		Search search = ((Search.Builder) ((Search.Builder) new Search.Builder(searchSourceBuilder.toString())
				.addIndex(elasticindex)).addType(typeToQuery)).build();
		LOG.debug("Search Query -->" + searchSourceBuilder.toString());
		JestResult result = null;
		try {
			result = jclient.execute(search);
			printJestResult(result);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}

	public static Integer getNumHits(JestResult result) {
		JsonObject json = result.getJsonObject();
		Integer hits = Integer.parseInt(json.getAsJsonObject("hits").get("total").toString());
		return hits;
	}

	public static void initJest(String url, int port) {
		String elasticAddress = url + ":" + port;
		JestClientFactory factory = new JestClientFactory();
		factory.setHttpClientConfig(new HttpClientConfig.Builder(elasticAddress).multiThreaded(true).build());
		jclient = factory.getObject();
	}

	public static void testAll() {
		// search("user", "firstName", "Jimmy");
		search("user", "firstName", "John");
		// search("user", "lastName", "Hendrix");
		search("country", "name", "par");
		search("organization", "preferred-name", "par");
		search("organization", "preferred-name", "lpar");
	}

	public static void createAllTypes() {
		createIndex(settingsJson, elasticindex);
		createType(userMapping, elasticindex, USER_TYPE);
		createType(countryMapping, elasticindex, COUNTRY_TYPE);
		createType(orgMapping, elasticindex, ORG_TYPE);
	}
	
	public static void loadAllTypes(){
		loadUsersBulk(userDataPath);
		loadType(countries, "country");
		loadType(organizations, "organization");
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException {
		opArgs = args;
		init("http://localhost", 9200, null);
		
		System.out.println("JSB : " + elasticUrl);
		if (cmdArgs.hasOption(dumpOp)) {
			dumpOptions();
		}

		if (cmdArgs.hasOption(makeUserEmpty)) {
			LOG.info("Executing User index and type creation without load...");
			createIndex(settingsJson, elasticindex);
			createType(userMapping, elasticindex, USER_TYPE);
		}
		else if (cmdArgs.hasOption(makeUser)) {
			LOG.info("Executing User index and type creation WITH load...");
			createIndex(settingsJson, elasticindex);
			createType(userMapping, elasticindex, USER_TYPE);
			loadUsersBulk(userDataPath);
		}

		if (cmdArgs.hasOption(makeCountryEmpty)) {
			LOG.info("Executing Country index and type creatioos.namen without load...");
			createIndex(settingsJson, elasticindex);
			createType(countryMapping, elasticindex, COUNTRY_TYPE);
		}
		else if (cmdArgs.hasOption(makeCountry)) {
			LOG.info("Executing Country index and type creation WITH load...");
			createIndex(settingsJson, elasticindex);
			createType(countryMapping, elasticindex, COUNTRY_TYPE);
			loadType(countries, "country");
		}

		if (cmdArgs.hasOption(makeOrgEmpty)) {
			LOG.info("Executing User index and type creation without load...");
			createIndex(settingsJson, elasticindex);
			createType(orgMapping, elasticindex, ORG_TYPE);
		}
		else if (cmdArgs.hasOption(makeOrg)) {
			LOG.info("Executing User index and type creation WITH load...");
			createIndex(settingsJson, elasticindex);
			createType(orgMapping, elasticindex, ORG_TYPE);
			loadType(organizations, "organization");
		}

		if (cmdArgs.hasOption(makeAllEmpty)) {
			LOG.info("Executing index and type creation only...");
			createAllTypes();
		}
		else if (cmdArgs.hasOption(makeAllOp)) {
			LOG.info("Executing index and type creation WITH load...");
			createAllTypes();
			loadAllTypes();
		}
		if (cmdArgs.hasOption(testOp)) {
			LOG.info("Performing simple tests on index and types...");
			loadSingleMockUser();
			testAll();
		}
		shutdown();
	}
}
