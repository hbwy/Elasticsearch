package com.wy.elasticsearch.utils;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.Node;

public class MyUtils {
	public static TransportClient getClient() {
		//	Settings settings = ImmutableSettings.settingsBuilder()
		//				.put("cluster.name", "myClusterName")
		//				.build();
		//	Client client = new TransportClient(settings);
		
		TransportClient client = new TransportClient()
			.addTransportAddress(new InetSocketTransportAddress("localhost",9300));
		  //.addTransportAddress(new InetSocketTransportAddress("host1", 9300))
		return client;
	}

	public static Node getNode(){
		return nodeBuilder().node();
	}
	
	public static void closeClient(TransportClient client) {
		client.close();
	}
	
	public static void closeNode(Node node){
		node.close();
	}

	public static Client getNodeClient() {

		//设置集群的名字
		//Node node = nodeBuilder().clusterName("yourclustername").node();

		//Node node = nodeBuilder().client(true).node();
		//Node node = nodeBuilder().local(true).node();

		Node node = nodeBuilder().node();
		Client client = node.client();
		return client;
	}

	public static void closeNodeClient(Client client) {
		client.close();
	}
}
