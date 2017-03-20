package com.thomson.onep.mock;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
//import java.util.UUID;

import java.util.Random;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class LoadMockElasticUsers {

	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		// Generated using www.mockaroo.com
		String filePath = new File("").getAbsolutePath() + "\\src\\test\\resources\\elastic_users_mock_data.json";

		// on startup
		Node node = nodeBuilder().client(true).node();
		Client client = node.client();

		BulkRequestBuilder bulkRequest = client.prepareBulk();

		try {
			// read the json file
			FileReader reader = new FileReader(filePath);
			JSONParser jsonParser = new JSONParser();
			JSONArray users = (JSONArray) jsonParser.parse(reader);

			Iterator<JSONObject> usersIterator = users.iterator();

			while (usersIterator.hasNext()) {
				JSONObject user = usersIterator.next();

				JSONArray interest = (JSONArray) user.get("interest");
				int randomNum = new Random().nextInt(user.size());
				ArrayList<String> interestsList = new ArrayList<String>(randomNum);
				if (randomNum > 0) {
					for (int i = 0; i < randomNum; i++) {
						interestsList.add(interest.get(i).toString());
					}
				}

				bulkRequest.add(client.prepareIndex("profiles", "users",
				// UUID.randomUUID().toString())
						user.get("id").toString()).setSource(
						jsonBuilder().startObject().field("firstName", user.get("firstName"))
								.field("middleName", user.get("middleName")).field("lastName", user.get("lastName"))
								.field("primaryInstitution", user.get("primaryInstitution"))
								.field("location", user.get("location")).field("role", user.get("role"))
								.field("interest", interestsList).endObject()));
			}

			BulkResponse bulkResponse = bulkRequest.execute().actionGet();
			if (bulkResponse.hasFailures()) {
				// process failures by iterating through each bulk response item
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		// on shutdown
		node.close();

	}

}
