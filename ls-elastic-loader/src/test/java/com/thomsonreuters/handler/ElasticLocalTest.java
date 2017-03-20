package com.thomsonreuters.handler;

import static org.junit.Assert.assertTrue;
import io.searchbox.client.JestResult;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.thomsonreuters.util.ElasticUtils;

@Ignore
public class ElasticLocalTest {
	private static ElasticUtils elastic;
	private static final String INDEX = "profiles";
	private static final String EDGE_ANALYZER = "autocomplete_edge_analyzer";
	private static final String NGRAM_ANALYZER = "autocomplete_ngram_analyzer";
	private static final String USER_TYPE = "user";
	private static final String ORG_TYPE = "organization";
	private static final String COUNTRY_TYPE = "country";
	private static final String USER_SEARCH_TERM = "jo";// joe, jonathan, joleen
	private static final String COUNTRY_SEARCH_TERM = "par"; // paraquay,
																// rampart
	private static final String ORG_SEARCH_TERM = "lpar"; // Valparaiso,
															// Alparslan
	private static final String USER_FIELD = "firstName";
	private static final String COUNTRY_FIELD = "name";
	private static final String ORG_FIELD = "preferred-name";
	private static final int N_EXPECTED_USER_HITS = 36;
	private static final int N_EXPECTED_ORG_HITS = 37;
	private static final int N_EXPECTED_COUNTRY_HITS = 4;

	@BeforeClass
	public static void initialize() {
		elastic = new ElasticUtils();
	}

	@Ignore
	public void createIndexAndTypes() {
		createIndex();
		createUserType();
		createCountryType();
		createOrganizationType();
	}

	@Test
	public void testSearchQuery() {
		// Actual search results returned defaults to 10 when the total # hits
		// are > 10. Response contains the actual number of hits

		// user type currently uses edge ngram. should match "JOseph", "JOn",
		// but not "reJOice"
		JestResult userResponse = elastic.search(USER_TYPE, USER_FIELD, USER_SEARCH_TERM);
		Integer userHits = elastic.getNumHits(userResponse);

		JestResult countryResponse = elastic.search(COUNTRY_TYPE, COUNTRY_FIELD, COUNTRY_SEARCH_TERM);
		Integer countryHits = elastic.getNumHits(countryResponse);

		JestResult orgResponse = elastic.search(ORG_TYPE, ORG_FIELD, ORG_SEARCH_TERM);
		Integer orgHits = elastic.getNumHits(orgResponse);

		// Search results returned for all queries
		assertTrue(userHits == N_EXPECTED_USER_HITS);
		assertTrue(countryHits == N_EXPECTED_COUNTRY_HITS);
		assertTrue(orgHits == N_EXPECTED_ORG_HITS);
	}

	public void createIndex() {
		JestResult response = elastic.createIndex(elastic.settingsJson, INDEX);
		assertTrue(response.isSucceeded());
	}

	public void createUserType() {
		JestResult putResponse = elastic.createType(elastic.userMapping, INDEX, USER_TYPE);
		assertTrue(putResponse.isSucceeded());
		JestResult loadResponse = elastic.loadUsersBulk(elastic.userDataPath);
		assertTrue(loadResponse.isSucceeded());
	}

	public void createCountryType() {
		JestResult putResponse = elastic.createType(elastic.countryMapping, INDEX, COUNTRY_TYPE);
		assertTrue(putResponse.isSucceeded());
		JestResult loadResponse = elastic.loadType(elastic.countries, COUNTRY_TYPE);
		assertTrue(loadResponse.isSucceeded());
	}

	public void createOrganizationType() {
		JestResult putResponse = elastic.createType(elastic.orgMapping, INDEX, ORG_TYPE);
		assertTrue(putResponse.isSucceeded());
		JestResult loadResponse = elastic.loadType(elastic.organizations, ORG_TYPE);
		assertTrue(loadResponse.isSucceeded());
	}

}