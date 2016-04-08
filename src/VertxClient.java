
import java.io.*;

import org.apache.http.HttpResponse;
// import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
// import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClientBuilder;
// import org.apache.http.message.BasicNameValuePair;
/*
* @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
*/
public class VertxClient{
// Convenience method so you can run it in your IDE
	public static void main(String[] args) throws IOException{
		
		String url = "http://localhost:8080/delete?a";

		HttpClient client = HttpClientBuilder.create().build();
		HttpGet request = new HttpGet(url);

		// add request header
		// private final String USER_AGENT = "Mozilla/5.0";
		// request.addHeader("User-Agent", USER_AGENT);

		HttpResponse response = client.execute(request);
		System.out.println("Got response");
		BufferedReader rd = new BufferedReader(
				new InputStreamReader(response.getEntity().getContent()));

		StringBuffer result = new StringBuffer();
		String line = "";
		while ((line = rd.readLine()) != null) {
			result.append(line);
		}
		System.out.println(result);

	}
}
