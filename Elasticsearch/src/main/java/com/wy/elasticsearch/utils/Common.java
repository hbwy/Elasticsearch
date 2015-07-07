package com.wy.elasticsearch.utils;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

public class Common {

	public static TransportClient client = MyUtils.getClient();
	public static Node node = MyUtils.getNode();

	public static void createIndex(String indexname, String type, String id, Map<String, Object> json) {

		IndexResponse response = client.prepareIndex(indexname, type, id)
				.setSource(json)
				.execute()
				.actionGet();
		MyUtils.closeClient(client);
		getIndexResponse(response);
	}

	public static void createIndex(String indexname, String type, String id, XContentBuilder builder) {

		IndexResponse response = client.prepareIndex(indexname, type, id)
				.setSource(builder)
				.execute()
				.actionGet();
		MyUtils.closeClient(client);
		getIndexResponse(response);
	}
	
	public static void createIndex(String indexname, String type, String id, byte[] json) {

		IndexResponse response = client.prepareIndex(indexname, type, id)
				.setSource(json)
				.execute()
				.actionGet();
		MyUtils.closeClient(client);
		getIndexResponse(response);
	}
	
	public static void createIndexResponse(String indexname, String type, List<String> jsondata) {
		//创建索引库 需要注意的是.setRefresh(true)这里一定要设置,否则第一次建立索引查找不到数据
		IndexRequestBuilder requestBuilder = client.prepareIndex(indexname, type)
				.setRefresh(true);
		for (int i = 0; i < jsondata.size(); i++) {
			requestBuilder.setSource(jsondata.get(i))
			.execute()
			.actionGet();
		}
		MyUtils.closeClient(client);
	}
	
	public static void createIndexResponse(String indexname, String type, String jsondata) {
		IndexResponse response = client.prepareIndex(indexname, type)
				.setSource(jsondata)
				.execute()
				.actionGet();
		getIndexResponse(response);
		MyUtils.closeClient(client);
	}
	
	public static void getIndex(String indexname,String type,String id) {
		GetResponse response = client.prepareGet("twitter1", "tweet1", "1")
				.execute()
				.actionGet();
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

	public static void searchIndex(String indexname, String filed, String keyword) {

		QueryBuilder qb = QueryBuilders.termQuery(filed, keyword);
		SearchResponse scrollResp = client.prepareSearch(indexname)
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
				Iterator<Entry<String, Object>> rpItor = hit.getSource()
						.entrySet()
						.iterator();
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
	
	public static void count(String indexname, Map<String, String> map) {
		CountResponse response = client.prepareCount(indexname)
				.setQuery(QueryBuilders.termQuery("user", map.get("user")))
				.execute()
				.actionGet();
		MyUtils.closeClient(client);
		System.out.println(response.getCount());
	}
	
	public static void aggregations() {
		SearchResponse sr = node.client()
				.prepareSearch()
				.setQuery(QueryBuilders.matchAllQuery())
				.addAggregation(AggregationBuilders.terms("agg1").field("field"))
				.addAggregation(AggregationBuilders.dateHistogram("agg2").field("birth").interval(DateHistogram.Interval.YEAR))
				.execute()
				.actionGet();
		// Get your facet results
		Terms agg1 = sr.getAggregations().get("agg1");
		DateHistogram agg2 = sr.getAggregations().get("agg2");
		MyUtils.closeNode(node);
	}
	
	public static void multiSearch() {
		SearchRequestBuilder srb1 = node.client()
				.prepareSearch()
				.setQuery(QueryBuilders.queryString("twitter"))
				.setSize(1);
		SearchRequestBuilder srb2 = node.client()
				.prepareSearch()
				.setQuery(QueryBuilders.matchQuery("user", "kimchy"))
				.setSize(1);

		MultiSearchResponse sr = node.client()
				.prepareMultiSearch()
				.add(srb1)
				.add(srb2)
				.execute()
				.actionGet();

		// You will get all individual responses from MultiSearchResponse#getResponses()
		for (MultiSearchResponse.Item item : sr.getResponses()) {
			SearchResponse response = item.getResponse();
			System.out.println(response.getHits().getTotalHits());
		}
		MyUtils.closeNode(node);
	}
	
	public static void deleteIndex(String indexname,String type,String id) {
		//删除索引名为twitter，类型为tweet，id为1的文档
		DeleteResponse response = client.prepareDelete(indexname, type, id)
				.execute()
				.actionGet();
		MyUtils.closeClient(client);
	}
	
	public void deleteByQuery(String indexname,Map<String,String> map) {
		DeleteByQueryResponse response = client.prepareDeleteByQuery(indexname)
				.setQuery(QueryBuilders.termQuery("_type", map.get("type")))
				.execute()
				.actionGet();
		System.out.println(response.status());
		MyUtils.closeClient(client);
	}
	
	public void updateIndex(String indexname,String type,String id,Map<String,String> filed){
		UpdateRequest updateRequest;
		try {
			updateRequest = new UpdateRequest(indexname, type, id).doc(
					jsonBuilder()
					.startObject()
					.field("gender", filed.get("gender"))
					.endObject()
					);
			client.update(updateRequest).get();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		
	}
	
	public static void bulk(){
		//The bulk API allows one to index and delete several documents in a single request
		BulkRequestBuilder bulkRequest = client.prepareBulk();

		// either use client#prepare, or use Requests# to directly build index/delete requests
		try {
			bulkRequest.add(client.prepareIndex("twitter", "tweet", "1").setSource(jsonBuilder()
					.startObject()
					.field("user", "kimchy")
					.field("postDate", new Date())
					.field("message", "trying out Elasticsearch")
					.endObject()));
			
			bulkRequest.add(client.prepareIndex("twitter", "tweet", "2").setSource(jsonBuilder()
					.startObject()
					.field("user", "kimchy")
					.field("postDate", new Date())
					.field("message", "another post")
					.endObject()));
		} catch (IOException e) {
			e.printStackTrace();
		}
		BulkResponse bulkResponse = bulkRequest.execute().actionGet();
		if (bulkResponse.hasFailures()) {
			// process failures by iterating through each bulk response item
		}
		BulkItemResponse[] brs = bulkResponse.getItems();
		for (int i = 0; i < brs.length; i++) {
			String index = brs[i].getIndex();
			String type = brs[i].getType();
			String id = brs[i].getId();
			long version = brs[i].getVersion();
			System.out.println(index+"\n"+type+"\n"+id+"\n"+version);
		}
	}
	
	public static void getIndexResponse(IndexResponse response) {
		// Index name  为索引库名，一个es集群中可以有多个索引库。 名称必须为小写
		String _index = response.getIndex();
		System.out.println(_index);
		// Type name  Type为索引类型，是用来区分同索引库下不同类型的数据的，一个索引库下可以有多个索引类型。
		String _type = response.getType();
		System.out.println(_type);
		// Document ID (generated or not)
		String _id = response.getId();
		System.out.println(_id);
		// Version (if it's the first time you index this document, you will get: 1)
		long _version = response.getVersion();
		System.out.println(_version);
		// isCreated() is true if the document is a new one, false if it has been updated
		boolean created = response.isCreated();
		System.out.println(created);
	}
}
