/*
 * Copyright 2016-2018 flexibledata.io.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */
package io.flexibledata.pipeline.output.elasticsearch;

/**
 * ES模板类
 *
 * @author tan.jie
 *
 */
@Getter
@Setter
public class ESTemplate implements ESOperations {

	private static final Logger LOGGER = LoggerFactory.getLogger(ESTemplate.class);
	private TransportClient client;
	private Long totalSize;

	public ESTemplate(ESClient esClient) throws UnknownHostException {
		this.client = esClient.getClient();
	}

	@Override
	public TransportClient getClient() {
		return client;
	}

	/**
	 * 无条件查询方法
	 * 
	 * @param index
	 *            索引名称
	 * @param type
	 *            类型名称
	 * @param mapper
	 *            映射接口实现类
	 * @param pageNum
	 *            分页号
	 * @param pageSize
	 *            分页大小
	 * @return
	 */
	public <T> List<T> query(String index, String type, RowMapper<T> mapper, Integer pageNum, Integer pageSize) {
		LOGGER.info("Enter into query method. index={}, type={}", index, type);
		SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setTypes(type).setSearchType(SearchType.QUERY_THEN_FETCH);
		SearchResponse searchResponse = null;
		if (pageNum != null && pageSize != null) {
			searchResponse = searchRequestBuilder.setFrom((pageNum - 1) * pageSize).setSize(pageSize).get();
		} else {
			searchResponse = searchRequestBuilder.get();
		}
		SearchHit[] hits = searchResponse.getHits().getHits();
		setTotalSize(searchResponse);
		List<T> rs = new ArrayList<T>();
		for (SearchHit searchHit : hits) {
			rs.add(mapper.mapRow(searchHit));
		}
		return rs;
	}

	/**
	 * 通过DSL条件查询方法
	 * 
	 * @param index
	 *            索引名称
	 * @param type
	 *            类型名称
	 * @param queryBuilder
	 *            dslQuery语句构造类
	 * @param mapper
	 *            映射接口实现类
	 * @return
	 */
	@Override
	public <T> List<T> queryWithFilter(String index, String type, QueryBuilder queryBuilder, RowMapper<T> mapper) {
		LOGGER.info("Enter into queryWithFilter method. index={}, type={}", index, type);
		SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setTypes(type).setSearchType(SearchType.QUERY_THEN_FETCH);

		searchRequestBuilder.setQuery(queryBuilder);
		SearchResponse searchResponse = searchRequestBuilder.get();

		SearchHit[] hits = searchResponse.getHits().getHits();
		setTotalSize(searchResponse);
		List<T> rs = new ArrayList<T>();

		for (SearchHit searchHit : hits) {
			rs.add(mapper.mapRow(searchHit));
		}
		return rs;
	}

	/**
	 * 通过DSL条件查询方法
	 * 
	 * @param index
	 *            索引名称
	 * @param type
	 *            类型名称
	 * @param queryBuilder
	 *            dslQuery语句构造类
	 * @param pageNum
	 *            分页号
	 * @param pageSize
	 *            分页大小
	 * @param mapper
	 *            映射接口实现类
	 * @return
	 */
	@Override
	public <T> List<T> queryWithFilter(String index, String type, QueryBuilder queryBuilder, Integer pageNum, Integer pageSize, RowMapper<T> mapper) {
		LOGGER.info("Enter into queryWithFilter method. index={}, type={}, pageNum={}, pageSize={}", index, type, pageNum, pageSize);
		SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setTypes(type).setSearchType(SearchType.QUERY_THEN_FETCH);

		searchRequestBuilder.setQuery(queryBuilder);
		SearchResponse searchResponse = null;
		if (pageNum != null && pageSize != null) {
			searchResponse = searchRequestBuilder.setFrom((pageNum - 1) * pageSize).setSize(pageSize).get();
		} else {
			searchResponse = searchRequestBuilder.get();
		}

		SearchHit[] hits = searchResponse.getHits().getHits();
		setTotalSize(searchResponse);
		List<T> rs = new ArrayList<T>();
		for (SearchHit searchHit : hits) {
			rs.add(mapper.mapRow(searchHit));
		}
		return rs;
	}

	/**
	 * 通过DSL条件查询方法
	 * 
	 * @param index
	 *            索引名称
	 * @param type
	 *            类型名称
	 * @param queryBuilder
	 *            dslQuery语句构造类
	 * @param pageNum
	 *            分页号
	 * @param pageSize
	 *            分页大小
	 * @param mapper
	 *            映射接口实现类
	 * @return
	 */
	public SearchHit[] queryWithFilter(String index, String type, QueryBuilder queryBuilder) {
		LOGGER.info("Enter into queryWithFilter method. index={}, type={}", index, type);
		SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setTypes(type).setSearchType(SearchType.QUERY_THEN_FETCH);
		searchRequestBuilder.setQuery(queryBuilder);
		SearchResponse searchResponse = searchRequestBuilder.get();
		return searchResponse.getHits().getHits();
	}

	/**
	 * 通过DSL条件查询方法
	 * 
	 * @param index
	 *            索引名称
	 * @param type
	 *            类型名称
	 * @param queryBuilder
	 *            dslQuery语句构造类
	 * @param sortField
	 *            排序字段名
	 * @param sortType
	 *            排序类型，"ASC"代表升序，"DESC"代码降序
	 * @param pageNum
	 *            分页号
	 * @param pageSize
	 *            分页大小
	 * @param mapper
	 *            映射接口实现类
	 * @return
	 */
	public <T> List<T> pagingAndSortQuery(String index, String type, QueryBuilder queryBuilder, String sortField, String sortType, Integer pageNum, Integer pageSize,
			RowMapper<T> mapper) {
		LOGGER.info("Enter into pagingAndSortQuery method. index={}, type={}, sortField={}, sortType={}, pageNum={}, pageSize={}", index, type, sortField, sortType, pageNum,
				pageSize);
		SearchRequestBuilder searchRequestBuilder = null;

		if (sortType.equals("ASC") || sortType.equals("asc")) {
			searchRequestBuilder = client.prepareSearch(index).setTypes(type).setSearchType(SearchType.QUERY_THEN_FETCH).addSort(sortField, SortOrder.ASC);
		} else {
			searchRequestBuilder = client.prepareSearch(index).setTypes(type).setSearchType(SearchType.QUERY_THEN_FETCH).addSort(sortField, SortOrder.DESC);
		}

		if (queryBuilder != null) {
			searchRequestBuilder.setQuery(queryBuilder);
		}

		SearchResponse searchResponse = searchRequestBuilder.setFrom((pageNum - 1) * pageSize).setSize(pageSize).get();
		SearchHit[] hits = searchResponse.getHits().getHits();
		setTotalSize(searchResponse);
		List<T> rs = new ArrayList<T>();
		for (SearchHit searchHit : hits) {
			rs.add(mapper.mapRow(searchHit));
		}
		return rs;
	}

	/**
	 * 通过DSL条件查询方法
	 * 
	 * @param index
	 *            索引名称
	 * @param type
	 *            类型名称
	 * @param queryBuilder
	 *            dslQuery语句构造类
	 * @param List<Sort>
	 *            排序列表
	 * @param pageNum
	 *            分页号
	 * @param pageSize
	 *            分页大小
	 * @param mapper
	 *            映射接口实现类
	 * @return
	 */
	public <T> List<T> pagingAndSortQuery(String index, String type, QueryBuilder queryBuilder, List<Sort> sortList, Integer pageNum, Integer pageSize, RowMapper<T> mapper) {
		LOGGER.info("Enter into pagingAndSortQuery method. index={}, type={}, sortList={}, pageNum={}, pageSize={}", index, type, sortList, pageNum, pageSize);

		SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setTypes(type).setSearchType(SearchType.QUERY_THEN_FETCH);

		if (sortList != null && !sortList.isEmpty()) {
			for (Sort each : sortList) {
				each.getFiled();
				each.getSortOrder();
				searchRequestBuilder.addSort(each.getFiled(), each.getSortOrder());
			}
		}

		if (queryBuilder != null) {
			searchRequestBuilder.setQuery(queryBuilder);
		}

		SearchResponse searchResponse = searchRequestBuilder.setFrom((pageNum - 1) * pageSize).setSize(pageSize).get();
		SearchHit[] hits = searchResponse.getHits().getHits();
		setTotalSize(searchResponse);
		List<T> rs = new ArrayList<T>();
		for (SearchHit searchHit : hits) {
			rs.add(mapper.mapRow(searchHit));
		}
		return rs;
	}

	/**
	 * 通过DSL条件查询方法
	 * 
	 * @param index
	 *            索引
	 * @param type
	 *            类型
	 * @param queryBuilder
	 *            DSL查询语句
	 * @param pageNum
	 *            页号
	 * @param pageSize
	 *            页大小
	 * @return 索引原始数据
	 */
	public SearchHit[] queryWithFilter(String index, String type, QueryBuilder queryBuilder, Integer pageNum, Integer pageSize) {
		LOGGER.info("Enter into queryWithFilter method. index={}, type={}", index, type);
		SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setTypes(type).setSearchType(SearchType.QUERY_THEN_FETCH);
		searchRequestBuilder.setQuery(queryBuilder);
		searchRequestBuilder.setFrom(pageNum - 1);
		searchRequestBuilder.setSize(pageSize);
		SearchResponse searchResponse = searchRequestBuilder.get();
		return searchResponse.getHits().getHits();
	}

	/**
	 * 索引文档
	 * 
	 * @param index
	 * @param type
	 * @param docId
	 * @param source
	 * @return
	 */
	public IndexResponse indexDoc(String index, String type, String docId, Map<String, Object> source) {
		return client.prepareIndex(index, type, docId).setRouting(docId).setSource(source).get();
	}

	/**
	 * 索引文档
	 * 
	 * @param index
	 * @param type
	 * @param docId
	 * @param parentId
	 * @param source
	 * @return
	 */
	public IndexResponse indexDocWithRouting(String index, String type, String docId, String parentId, Map<String, Object> source) {
		if (parentId == null || parentId.isEmpty()) {
			return indexDoc(index, type, docId, source);
		}
		return client.prepareIndex(index, type, docId).setParent(parentId).setRouting(parentId).setSource(source).get();
	}

	/**
	 * 更新文档
	 * 
	 * @param index
	 * @param type
	 * @param docId
	 * @param source
	 * @return
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public UpdateResponse updateDoc(String index, String type, String docId, Map<String, Object> source) throws InterruptedException, ExecutionException {
		UpdateRequest updateRequest = new UpdateRequest(index, type, docId);
		updateRequest.doc(source);
		return client.update(updateRequest).get();
	}

	/**
	 * 更新文档
	 * 
	 * @param index
	 * @param type
	 * @param docId
	 * @param parentId
	 * @param source
	 * @return
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public UpdateResponse updateDocWithRouting(String index, String type, String docId, String parentId, Map<String, Object> source)
			throws InterruptedException, ExecutionException {
		UpdateRequest updateRequest = new UpdateRequest(index, type, docId);
		updateRequest.parent(parentId);
		updateRequest.routing(parentId);
		updateRequest.doc(source);
		return client.update(updateRequest).get();
	}

	/**
	 * 删除文档
	 * 
	 * @param index
	 * @param type
	 * @param docId
	 * @return
	 */
	public DeleteResponse deleteDoc(String index, String type, String docId) {
		return client.prepareDelete(index, type, docId).get();
	}

	/**
	 * 删除文档
	 * 
	 * @param index
	 * @param type
	 * @param docId
	 * @param parentId
	 * @return
	 */
	public DeleteResponse deleteDoc(String index, String type, String docId, String parentId) {
		return client.prepareDelete(index, type, docId).setRouting(parentId).get();
	}

	/**
	 * 创建索引
	 * 
	 * @param index
	 * @return
	 */
	public CreateIndexResponse createIndex(String index) {
		return client.admin().indices().prepareCreate(index).get();
	}

	/**
	 * 创建索引映射
	 * 
	 * @param index
	 * @param type
	 * @param json
	 * @return
	 */
	public PutMappingResponse createIndexMapping(String index, String type, String json) {
		return client.admin().indices().preparePutMapping(index).setType(type).setSource(json, XContentType.JSON).get();
	}

	/**
	 * 创建索引映射
	 * 
	 * @param index
	 * @param json
	 * @return
	 */
	public PutMappingResponse createIndexMapping(String index, String json) {
		return client.admin().indices().preparePutMapping(index).setSource(json, XContentType.JSON).get();
	}

	/**
	 * 判断索引是否存在
	 * 
	 * @param indexName
	 * @return
	 */
	public boolean isExistsIndex(String indexName) {
		IndicesExistsResponse response = getClient().admin().indices().exists(new IndicesExistsRequest().indices(new String[] { indexName })).actionGet();
		return response.isExists();
	}

	// 判断类型是否存在
	public boolean isExistsType(String indexName, String indexType) {
		TypesExistsResponse response = getClient().admin().indices().typesExists(new TypesExistsRequest(new String[] { indexName }, indexType)).actionGet();
		return response.isExists();
	}

	public void setTotalSize(SearchResponse response) {
		totalSize = response.getHits().getTotalHits();
	}

	/**
	 * 更新索引
	 * 
	 * @param index
	 *            索引名称
	 * @param type
	 *            索引类型
	 * @param docId
	 *            文档Id
	 * @param source
	 */
	public void updateIndex(String index, String type, String docId, Map<String, Object> source) {
		UpdateRequest updateRequest = new UpdateRequest();
		updateRequest.index(index);
		updateRequest.type(type);
		updateRequest.id(docId);
		updateRequest.doc(source);
		try {
			client.update(updateRequest).get();
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 更新索引
	 * 
	 * @param index
	 * @param type
	 * @param docId
	 * @param script
	 */
	public void updateIndex(String index, String type, String docId, Script script) {
		UpdateRequest updateRequest = new UpdateRequest(index, type, docId).script(script);
		try {
			client.update(updateRequest).get();
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 统计数量
	 * 
	 * @param index
	 * @param type
	 * @param queryBuilder
	 * @return
	 */
	public Long count(String index, String type, QueryBuilder queryBuilder) {
		return client.prepareSearch(index).setTypes(type).setSize(0).setQuery(queryBuilder).get().getHits().getTotalHits();
	}

	/**
	 * 通过DSL条件查询和聚合
	 * 
	 * @param index
	 *            索引名称
	 * @param type
	 *            类型名称
	 * @param queryBuilder
	 *            dslQuery语句构造类
	 * @param aggregationBuilder
	 *            dslAggregateion语句构造类
	 * @return
	 */
	public Aggregations aggregate(String index, String type, QueryBuilder queryBuilder, AggregationBuilder aggregationBuilder) {
		LOGGER.info("Enter into pagingAndSortQuery method. index={}, type={}, queryBuilder={}, aggregationBuilder={}", index, type, queryBuilder, aggregationBuilder);

		SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setTypes(type).setSearchType(SearchType.QUERY_THEN_FETCH);

		if (queryBuilder != null) {
			searchRequestBuilder.setQuery(queryBuilder);
		}

		if (aggregationBuilder != null) {
			searchRequestBuilder.addAggregation(aggregationBuilder);
		}
		searchRequestBuilder.setSize(0);// 不需要返回query数据

		SearchResponse searchResponse = searchRequestBuilder.get();
		return searchResponse.getAggregations();
	}

	public void bulkIndex(BulkRequestBuilder bulkRequestBuilder) {
		BulkResponse bulkResponse = bulkRequestBuilder.get();
		if (bulkResponse.hasFailures()) {
			LOGGER.error("批量插入有誤！");
		}
	}

}
