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

import java.util.concurrent.Semaphore;
import voldemort.client.StoreClientFactory;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.ClientConfig;
import voldemort.versioning.Versioned;
import voldemort.versioning.Version;

public class MasterDriver extends AbstractVerticle{

	private LinkedHashMap<String, Address> peer_servers = new LinkedHashMap<String, Address>();
	private String local_name = "Master";
	private ParsedConfiguration result;
	public void start() {

	  	parseFile();

		String bootstrapUrl = "tcp://localhost:6666";
		StoreClientFactory factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(bootstrapUrl));
	  	StoreClient<String, String> client = factory.getStoreClient("test");

		

		HttpServer server = vertx.createHttpServer();
		server.requestHandler(new Handler<HttpServerRequest>() {
			public void handle(HttpServerRequest req) {
				int i;
				String uri = req.uri();
				String query = req.query();
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
			        req.netSocket().close();
				} else {
					if(command.equalsIgnoreCase("get")){
						Versioned<String> version = client.get(query);
						req.response().setStatusCode(200);
			        	req.response().headers()
			        		.add("Content-Length", String.valueOf(version.getValue().length()))
			            	.add("Content-Type", "text/html; charset=UTF-8");
						req.response().write(version.getValue());
						req.response().end();
						req.netSocket().close();
					} else if(command.equalsIgnoreCase("put")){
						int sem_init_value = -(peer_servers.size() - 1);
						final Semaphore completeWork = new Semaphore(sem_init_value); 
						for (Map.Entry<String, Address> entry : peer_servers.entrySet()) {
							//wait for other servers to respond and say a yes
							
							HttpClient httpClient = vertx.createHttpClient();
					        httpClient.getNow(entry.getValue().port, entry.getValue().ip, uri, new Handler<HttpClientResponse>() {

					            @Override
					            public void handle(HttpClientResponse httpClientResponse) {

					                httpClientResponse.bodyHandler(new Handler<Buffer>() {
					                    @Override
					                    public void handle(Buffer buffer) {
					                        completeWork.release();
					                    }
					                });
					            }
					        });
						}
						
						vertx.executeBlocking(future -> {
							try {
								completeWork.acquire();
							} catch(InterruptedException e){
								e.printStackTrace();
							}
							future.complete();
						}, res -> {
							Version ret_val = client.put(query.substring(0,query.indexOf('=')),query.substring(query.indexOf('=')+1));
							req.response().setStatusCode(200);
			        		req.response().headers()
			        			.add("Content-Length", String.valueOf(16))
			            		.add("Content-Type", "text/html; charset=UTF-8");

							req.response().write("write successful");
							req.response().end();
							req.netSocket().close();
						});
					} else if(command.equalsIgnoreCase("delete")){
						int sem_init_value = -(peer_servers.size() - 1);
						final Semaphore completeWork = new Semaphore(sem_init_value); 
						for (Map.Entry<String, Address> entry : peer_servers.entrySet()) {
							//wait for other servers to respond and say a yes
							
							HttpClient httpClient = vertx.createHttpClient();
					        httpClient.getNow(entry.getValue().port, entry.getValue().ip, uri, new Handler<HttpClientResponse>() {

					            @Override
					            public void handle(HttpClientResponse httpClientResponse) {

					                httpClientResponse.bodyHandler(new Handler<Buffer>() {
					                    @Override
					                    public void handle(Buffer buffer) {
					                        completeWork.release();
					                    }
					                });
					            }
					        });
						}
						
						vertx.executeBlocking(future -> {
							try {
								completeWork.acquire();
							} catch(InterruptedException e){
								e.printStackTrace();
							}
							future.complete();
						}, res -> {
							boolean success = client.delete(query);
							req.response().setStatusCode(200);
			            	if(success){
			            		req.response().headers()
			        				.add("Content-Length", String.valueOf(17))
			            			.add("Content-Type", "text/html; charset=UTF-8");
								req.response().write("delete successful");
			            	}
							else{
								req.response().headers()
			        				.add("Content-Length", String.valueOf(19))
			            			.add("Content-Type", "text/html; charset=UTF-8");
								req.response().write("delete unsuccessful");
							}

							req.response().end();
							req.netSocket().close();
						});
					} else {
						req.response().setStatusCode(200);
			        	req.response().headers()
			        		.add("Content-Length", String.valueOf(15))
			            	.add("Content-Type", "text/html; charset=UTF-8");
						
						req.response().write("invalid request");
						req.response().end();
						req.netSocket().close();
					}
				}
			}
		}).listen(8080);
		
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