package com.baao.laggards.Baao_Laggards;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.json.JSONArray;
import org.json.JSONObject;

public class BAAOLoginDetails {
	
	public static void main(String[] args) throws Exception {

		BAAOLoginDetails http = new BAAOLoginDetails();

		System.out.println("\nTesting 2 - Send Http POST request");
		http.sendPost();

	}

	// HTTP POST request
	private void sendPost() throws Exception {

		Connection hcon = DriverManager.getConnection("jdbc:hive2://hdprd1-edge-lb01:20000/mosdev", "hdpcscgsm","C$cPr0DGSM$123");

		ResultSet rs_max_date = hcon.createStatement()
				.executeQuery("select max(login_Date)  from mosdev.MOS_BAAO_LOGIN_FREQUENCY");
		ResultSet rs_week_Quater = hcon.createStatement().executeQuery("select fiscal_week_in_qtr_num_int,fiscal_quarter_name from reference_tdprod_datalakepvwdb.pv_fiscal_week_to_year pw2y where pw2y.current_fiscal_week_flag='Y'");
		
		
		long startTime = 0L;
		while (rs_max_date.next()) {
			if(rs_max_date.next()) {
			Timestamp rsts = rs_max_date.getTimestamp(1);
			startTime = rsts.getTime();
			System.out.println("Start Time :: " + rs_max_date.getTimestamp(1));
			}
		}
		
		
		int week_num=0;
		String fiscal_quarter="";
		while (rs_week_Quater.next()) {
			week_num=rs_week_Quater.getInt(1);
			fiscal_quarter=rs_week_Quater.getString(2);
		}
		
		Date now = new java.util.Date();		
		Timestamp lastref = new java.sql.Timestamp(now.getTime());
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		String formattedDt = formatter.format(lastref);
		Timestamp lastRefresh = Timestamp.valueOf(formattedDt);
		System.out.println("End Time :: " + lastRefresh);
			

		String url = "http://analytics.api.appdynamics.com/events/query?start=" + startTime + "&end=" + lastRefresh;

		URL obj = new URL(url);
		HttpURLConnection con = (HttpURLConnection) obj.openConnection();

		// add reuqest header
		con.setRequestMethod("POST");
		con.setRequestProperty("Accept", "application/vnd.appd.events+json;v=2");
		con.setRequestProperty("Content-type", "application/vnd.appd.events+text;v=2");
		con.setRequestProperty("X-Events-API-AccountName", "cisco1_d5ce4a50-0e7c-4e42-a5a5-4c7d1bcfcd74");
		con.setRequestProperty("X-Events-API-Key", "aa9d9b55-4993-4f91-9691-623a393ce01d");

		String urlParameters = "SELECT timestamp as eventTimestamp, userdata.CECID AS \"CECID\", userdata.RoleName AS \"RoleName\", userdata.Email AS \"Email\", userdata.Company AS \"Company\",  distinctcount(pagename) AS \"Page Name (Count Distinct)\" FROM browser_records WHERE pagename IN (\"baao#landingp age\",\"baao#supplierlandingpage\",\"baao#negotiationdashboard\") LIMIT 999, 999, 999, 999, 999";
		// Send post request
		con.setDoOutput(true);
		DataOutputStream wr = new DataOutputStream(con.getOutputStream());
		wr.writeBytes(urlParameters);
		wr.flush();
		wr.close();

		int responseCode = con.getResponseCode();
		BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
		String inputLine;
		StringBuffer response = new StringBuffer();

		while ((inputLine = in.readLine()) != null) {
			response.append(inputLine);
		}
		in.close();

		// print result
		System.out.println(response.toString());

		JSONArray jsonArr = new JSONArray(response.toString());

		for (int i = 0; i < jsonArr.length(); i++) {
			JSONObject jsonObj = jsonArr.getJSONObject(i);
			JSONArray resultList = jsonObj.getJSONArray("results");
			System.out.println("result :: " + resultList.length());
			for (int j = 0; j < resultList.length(); j++) {
				JSONArray jsonObj2 = resultList.getJSONArray(j);
				System.out.println("result :: " + jsonObj2.getString(0));
				long ldate = jsonObj2.getLong(0);
				Date loginDate = new Date(ldate);
				System.out.println("login date " + loginDate);

				String formattedDate = formatter.format(loginDate);
				Timestamp timeStamp = Timestamp.valueOf(formattedDate);
				System.out.println("Formated date in string " + formattedDate);

				Date date1 = formatter.parse(formattedDate);
				System.out.println("new formated date" + date1);
				String cecId = jsonObj2.getString(1);
				// System.out.println("Cecid -->"+resultList );
				if (jsonObj2.getString(1) != "null") {

					String roleName = jsonObj2.getString(2);
					String eMail = jsonObj2.getString(3);
					String company = jsonObj2.getString(4);
					// System.out.println("roleName -->"+roleName );
					String user_type = "Cisco";
					if (company.startsWith("Cisco")) {
						user_type = "INTERNAL";
						company = company.replace("%2C", " ");
					} else {
						user_type = "EXTERNAL";
						System.out.println("cecId in loop ->" + cecId);
						if (cecId.indexOf("@") >= 0) {
							cecId = cecId.substring(0, cecId.indexOf("@"));
						}
					}

					System.out.println("cecId ->" + cecId);
					Statement stmt = hcon.createStatement();
					
					
					String lInsert = "INSERT INTO TABLE MOSDEV.MOS_BAAO_LOGIN_FREQUENCY  VALUES (\""
							+ cecId + "\",\"" + eMail + "\" ," + "\"" + roleName + "\" ,\"" + company + "\",\""
							+ user_type + "\", \"" + week_num + "\",\"" + fiscal_quarter + "\",\"" + timeStamp + "\", \"" + lastRefresh + "\" )";
					System.out.println(lInsert);

					// stmt.execute(lInsert);
					stmt.close();
					// hcon.commit();

				}
			}
			hcon.close();
		}

		System.out.println("End of the program");

	}
	

}
