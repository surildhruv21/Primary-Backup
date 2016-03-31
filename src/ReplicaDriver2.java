import java.util.Map;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import java.util.*;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.File;
import java.io.FileInputStream;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientResponse;

import com.hazelcast.core.*;
import com.hazelcast.config.*;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;

public class ReplicaDriver2 extends AbstractVerticle{

	private HashMap<String, Address> peer_servers = new HashMap<String, Address>();
	private String local_name = "Server3";
	private int server_comm_port;
	private ParsedConfiguration result;
	public void start() {
		// System.out.println("Enter the server name");
		// Scanner sc = new Scanner(System.in);
		// local_name = sc.nextLine();
	  	parseFile();


		// Config hazelcastConfig = new Config();
		// hazelcastConfig.getNetworkConfig().setPort( 5900 );
		// // Now set some stuff on the config (omitted)

		// ClusterManager mgr = new HazelcastClusterManager(hazelcastConfig);

		// VertxOptions options = new VertxOptions().setClusterManager(mgr);

		// Vertx.clusteredVertx(options, res -> {
		// 	if (res.succeeded()) {
		// 		// Vertx vertx = res.result();
		// 		int i = 0;
		// 	} else {
		// 	// failed!
		// 	}
		// });
		


		HttpServer server = vertx.createHttpServer();
		server.requestHandler(new Handler<HttpServerRequest>() {
			public void handle(HttpServerRequest req) {
			//        System.out.println("Got request: " + req.uri());
			//        System.out.println("Headers are: ");
			//        for (Map.Entry<String, String> entry : req.headers()) {
			//          System.out.println(entry.getKey() + ":" + entry.getValue());
			//        }



				int i;
				String uri = req.uri();
				String command = "";
				if(uri.contains("?")){
					command = uri.substring(1,uri.indexOf('?'));
				}
				if(command.equals("")){
					req.response().setStatusCode(200);
			        req.response().headers()
			            .add("Content-Length", String.valueOf(15))
			            .add("Content-Type", "text/html; charset=UTF-8");
			        req.response().write("invalid command");
			        
				}
				else{
					
					req.response().setStatusCode(200);
			        req.response().headers()
			        	.add("Content-Length", String.valueOf(15))
			            .add("Content-Type", "text/html; charset=UTF-8");
			        System.out.println(command);
					if(command.equalsIgnoreCase("get")){
						req.response().write("get successful");
						//get the result from database
					} else if(command.equalsIgnoreCase("writeValue")){
						System.out.println("Value to be written:"+ uri.substring(uri.indexOf('?')+1));
						req.response().write("put successful!");
						//perform consensus with other servers and get quorum and then push the changes
					} else if(command.equalsIgnoreCase("update")){
						req.response().write("update successful");
						//perform consensus with other servers and get quorum and then push the changes
					} else {
						req.response().write("invalid request");
					}
				}
				req.response().end();
			       // req.response().headers().set("Content-Type", "text/html; charset=UTF-8");
			       // req.response().end("<html><body><h1>Hello from vert.x!</h1></body></html>");
			}
		}).listen(server_comm_port);
		
	}

	public void parseFile() {
		// The path of your YAML file.
		final String fileName = "../configuration.yaml";
		try {
			InputStream ios = new FileInputStream(new File(fileName));
			Constructor c = new Constructor(ParsedConfiguration.class);
			Yaml yaml = new Yaml(c);
			result = (ParsedConfiguration) yaml.load(ios);
			for (User user : result.configuration) {
				if(local_name.equalsIgnoreCase(user.name)){
					server_comm_port = user.port;
					System.out.println(local_name + " " + user.name + " " + user.ip);
					continue;
				} else {
					peer_servers.put(user.name, new Address(user.ip, user.port));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}