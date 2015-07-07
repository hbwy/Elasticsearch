package com.wy.elasticsearch.main;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wy.elasticsearch.entity.Data;
import com.wy.elasticsearch.utils.Common;

public class Main {

	@Test
	public void testCreateIndex1() {
		//用map构造json串
		Map<String, Object> json = new HashMap<String, Object>();
		json.put("user", "kimchy");
		json.put("postDate", new Date());
		json.put("message", "trying out Elastic Search");
		Common.createIndex("twitter", "tweet", "1", json);
	}

	@Test
	public void testCreateIndex2() {
		//利用elasticsearch提供的方法构造json
		XContentBuilder builder = null;
		try {
			builder = jsonBuilder().startObject().field("user", "kimchy").field("postDate", new Date())
					.field("message", "trying out Elastic Search").endObject();
		} catch (IOException e) {
			e.printStackTrace();
		}
		Common.createIndex("twitter", "tweet", "1", builder);
	}

	@Test
	public void testCreateIndex3() {
		//用jackson构造json串
		//创建一个mapper对象
		ObjectMapper mapper = new ObjectMapper(); // create once, reuse

		Data data = new Data();
		data.user = "kimchy";
		data.postDate = new Date();
		data.message = "trying out Elasticsearch";

		byte[] json = null;
		try {
			json = mapper.writeValueAsBytes(data);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		Common.createIndex("twitter", "tweet", "1", json);
	}

	@Test
	public void testSearchIndex() {
		Common.searchIndex("twitter", "_type", "tweet");
	}

	@Test
	public void testCreateIndexResponse1() {
		ObjectMapper mapper = new ObjectMapper(); // create once, reuse

		Data data = new Data();
		data.user = "kimchy";
		data.postDate = new Date();
		data.message = "trying out Elasticsearch";

		byte[] json = null;
		try {
			json = mapper.writeValueAsBytes(data);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		String jsondata = new String(json);
		Common.createIndexResponse("twitter", "tweet", jsondata);
	}

	@Test
	public void testCreateIndexResponse2() {
		ObjectMapper mapper = new ObjectMapper(); // create once, reuse

		Data data1 = new Data();
		data1.user = "kimchy";
		data1.postDate = new Date();
		data1.message = "trying...";

		Data data2 = new Data();
		data2.user = "kimchy";
		data2.postDate = new Date();
		data2.message = "trying...";

		byte[] json1 = null;
		byte[] json2 = null;
		try {
			json1 = mapper.writeValueAsBytes(data1);
			json2 = mapper.writeValueAsBytes(data2);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		String jsondata1 = new String(json1);
		String jsondata2 = new String(json2);

		List<String> jsondata = new ArrayList<String>();
		jsondata.add(jsondata1);
		jsondata.add(jsondata2);

		Common.createIndexResponse("twitter", "tweet", jsondata);
	}

	@Test
	public void tesMultiSearch() {
		Common.multiSearch();
	}

	@Test
	public void testBulk() {
		Common.bulk();
	}

	@Test
	public void testCount() {
		Map<String,String> map = new HashMap<String,String>();
		map.put("user", "kimchy");
		Common.count("twitter", map);
	}
}
