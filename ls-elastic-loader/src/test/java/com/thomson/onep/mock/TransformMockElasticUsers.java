package com.thomson.onep.mock;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class TransformMockElasticUsers {

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws IOException {
		// Generated using www.mockaroo.com
		String filePath = new File("").getAbsolutePath()
				+ "\\src\\test\\resources\\elastic_users_mock_data.json";
		String transformedFilePath = new File("").getAbsolutePath()
				+ "\\src\\test\\resources\\elastic_users_mock_data_transformed.json";
		FileWriter file = new FileWriter(transformedFilePath);

		try {
			// read the json file
			FileReader reader = new FileReader(filePath);
			JSONParser jsonParser = new JSONParser();
			JSONArray users = (JSONArray) jsonParser.parse(reader);

			Iterator<JSONObject> usersIterator = users.iterator();

			while (usersIterator.hasNext()) {
				JSONObject user = usersIterator.next();

				JSONObject idObj = new JSONObject();
				idObj.put("_id", user.get("id"));

				JSONObject indexObj = new JSONObject();
				indexObj.put("index", idObj);

				JSONObject userObj = new JSONObject();
				userObj.put("firstName", user.get("firstName"));
				userObj.put("middleName", user.get("middleName"));
				userObj.put("lastName", user.get("lastName"));
				userObj.put("primaryInstitution",
						user.get("primaryInstitution"));
				userObj.put("location", user.get("location"));
				userObj.put("role", user.get("role"));

				JSONArray interest = (JSONArray) user.get("interest");
				int randomNum = new Random().nextInt(user.size());
				if (randomNum > 0) {
					ArrayList<String> interestsList = new ArrayList<String>(randomNum); 
					for (int i = 0; i < randomNum; i++) {
						interestsList.add(interest.get(i).toString());
					}
				
					userObj.put("interest", interestsList);
				}

				file.write(indexObj.toJSONString()
						+ System.getProperty("line.separator")
						+ userObj.toJSONString()
						+ System.getProperty("line.separator"));
			}

		} catch (Exception e) {
			e.printStackTrace();

		} finally {
			file.flush();
			file.close();
		}
	}
}
