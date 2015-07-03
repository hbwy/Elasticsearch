package com.wy.elasticsearch.main;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.junit.Test;

import com.wy.elasticsearch.utils.MyUtils;

public class Main {

	TransportClient client = MyUtils.getClient();

	@Test
	public void generateIndex() {
		
		//      用jackson构造json串
		//		创建一个mapper对象
		//		ObjectMapper mapper = new ObjectMapper(); // create once, reuse
		//		新建一个json串
		//		byte[] json = mapper.writeValueAsBytes(yourbeaninstance);
		
		//      利用elasticsearch提供的方法构造json
		//		XContentBuilder builder = jsonBuilder()
		//			    .startObject()
		//			        .field("user", "kimchy")
		//			        .field("postDate", new Date())
		//			        .field("message", "trying out Elasticsearch")
		//			    .endObject();
		
		//   用map构造json串
		Map<String, Object> json = new HashMap<String, Object>();
		json.put("user", "kimchy");
		json.put("postDate", new Date());
		json.put("message", "trying out Elastic Search");
		
		IndexResponse response = this.client.prepareIndex("twitter", "tweet", "1").setSource(json).execute()
				.actionGet();
		System.out.println(response);
		MyUtils.closeClient(client);
		
		//		// Index name
		//		String _index = response.getIndex();
		//		// Type name
		//		String _type = response.getType();
		//		// Document ID (generated or not)
		//		String _id = response.getId();
		//		// Version (if it's the first time you index this document, you will get: 1)
		//		long _version = response.getVersion();
		//		// isCreated() is true if the document is a new one, false if it has been updated
		//		boolean created = response.isCreated();
	}

	@Test
	public void getIndex() {
		GetResponse response = client.prepareGet("twitter", "tweet", "1").execute().actionGet();
		Map<String, Object> rpMap = response.getSource();
		if (rpMap == null) {
			System.out.println("empty");
			return;
		}
		Iterator<Entry<String, Object>> rpItor = rpMap.entrySet().iterator();
		while (rpItor.hasNext()) {
			Entry<String, Object> rpEnt = rpItor.next();
			System.out.println(rpEnt.getKey() + " : " + rpEnt.getValue());
		}
		MyUtils.closeClient(client);
	}

	@Test
	public void searchIndex() {

		QueryBuilder qb = QueryBuilders.termQuery("user", "kimchy");
		SearchResponse scrollResp = client.prepareSearch("twitter")
										  .setSearchType(SearchType.SCAN)
										  .setScroll(new TimeValue(60000))
										  .setQuery(qb)
										  .setSize(100)
										  .execute()
										  .actionGet(); 
		while (true) {
			scrollResp = client.prepareSearchScroll(scrollResp.getScrollId())
							   .setScroll(new TimeValue(600000))
							   .execute()
							   .actionGet();
			for (SearchHit hit : scrollResp.getHits()) {
				Iterator<Entry<String, Object>> rpItor = hit.getSource().entrySet().iterator();
				while (rpItor.hasNext()) {
					Entry<String, Object> rpEnt = rpItor.next();
					System.out.println(rpEnt.getKey() + " : " + rpEnt.getValue());
				}
			}
			if (scrollResp.getHits().hits().length == 0) {
				break;
			}
		}
		MyUtils.closeClient(client);
	}
	
	@Test
	public void deleteIndex() {
		DeleteResponse response = client.prepareDelete("twitter", "tweet", "1")
										.execute()
										.actionGet();
		System.out.println(response);
		MyUtils.closeClient(client);
	}

}
