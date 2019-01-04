package com.aliyun.phoenix.entity;

import com.google.common.collect.Lists;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

/**
 * 与具体ORM实现无关的分页查询结果封装.
 * 
 * @param <T> Page中记录的类型.
 * 
 */
public class Page<T> implements Iterable<T> {
	
	private static final Logger logger = LoggerFactory.getLogger(Page.class);
	/** 当前页的页号, 序号从1开始, 默认为1. */
	protected int pageNo = 1;
	/** 每页的记录数量, 默认为20 */
	protected int pageSize = 20;

	protected String orderBy = null;
	protected String orderDir = null;

	/** 是否默认计算总记录数 */
	protected boolean countTotal = true;

	/** 页内的记录列表 */
	protected List<T> result = null;
	/** 总记录数, 默认值为-1 */
	protected long totalCount = -1;
	
	protected String jsonResult;

	public Page() {
	}

	public Page(int pageNo, int pageSize) {
		this.pageNo = pageNo;
		this.pageSize = pageSize;
		if(pageSize > 50){
			logger.warn("pageSize > 50, pageSize=" + pageSize);
		}
	}

	public Page(int pageNo, int pageSize, String orderBy, String orderDir) {
		this.pageNo = pageNo;
		this.pageSize = pageSize;
		this.orderBy = orderBy;
		this.orderDir = orderDir;
		if(pageSize > 50){
			logger.warn("pageSize > 50, pageSize=" + pageSize);
		}
	}

	/**
	 * 获得当前页的页号, 序号从1开始, 默认为1.
	 */
	public int getPageNo() {
		if(pageSize > 50){
			logger.warn("pageSize > 50, pageSize=" + pageSize);
		}
		return pageNo;
	}

	/**
	 * 设置当前页的页号, 序号从1开始, 低于1时自动调整为1.
	 */
	public void setPageNo(final int pageNo) {
		this.pageNo = pageNo;
		if (pageNo < 1) {
			this.pageNo = 1;
		}
		if(pageSize > 50){
			logger.warn("pageSize > 50, pageSize=" + pageSize);
		}
	}

	/**
	 * 获得每页的记录数量, 默认为10.
	 */
	public int getPageSize() {
		return pageSize;
	}

	/**
	 * 设置每页的记录数量, 低于1时自动调整为1.
	 */
	public void setPageSize(final int pageSize) {
		this.pageSize = pageSize;

		if (pageSize < 1) {
			this.pageSize = 1;
		}
	}

	/**
	 * 获得排序字段, 无默认值. 多个排序字段时用','分隔.
	 */
	public String getOrderBy() {
		return orderBy;
	}

	/**
	 * 设置排序字段, 多个排序字段时用','分隔.
	 */
	public void setOrderBy(final String orderBy) {
		this.orderBy = orderBy;
	}

	/**
	 * 获得排序方向, 无默认值.
	 */
	public String getOrderDir() {
		return orderDir;
	}

	/**
	 * 设置排序方式向.
	 * 
	 * @param orderDir
	 *            可选值为desc或asc,多个排序字段时用','分隔.
	 */
	public void setOrderDir(final String orderDir) {
		String lowcaseOrderDir = StringUtils.lowerCase(orderDir);

		// 检查order字符串的合法值
		String[] orderDirs = StringUtils.split(lowcaseOrderDir, ',');
		for (String orderDirStr : orderDirs) {
			if (!StringUtils.equals(Sort.DESC, orderDirStr) && !StringUtils.equals(Sort.ASC, orderDirStr)) {
				throw new IllegalArgumentException("排序方向" + orderDirStr + "不是合法值");
			}
		}

		this.orderDir = lowcaseOrderDir;
	}

	/**
	 * 获取排序字符串
	 */
	public String getSortStr() {
		String sortStr = "";
		List<Sort> sortList = getSort();
		for (int i = 0; i < sortList.size(); i++) {
			Sort sort = sortList.get(i);
			if (i != 0) {
				sortStr += ",";
			}
			sortStr += sort.getProperty() + " " + sort.getDir();
		}
		return sortStr;
	}

	/**
	 * 获得排序参数.
	 */
	public List<Sort> getSort() {
		List<Sort> orders = Lists.newArrayList();
		if (orderBy == null) {
			return orders;
		} 
		
		String[] orderBys = StringUtils.split(orderBy, ',');
		String[] orderDirs = StringUtils.split(orderDir, ',');
		// "分页多重排序参数中,排序字段与排序方向的个数不相等");
		if (orderBys.length != orderDirs.length) {
			throw new IllegalArgumentException("分页多重排序参数中,排序字段与排序方向的个数不相等");
		}
		for (int i = 0; i < orderBys.length; i++) {
			orders.add(new Sort(orderBys[i], orderDirs[i]));
		}
		return orders;
	}

	/**
	 * 是否已设置排序字段,无默认值.
	 */
	public boolean isOrderBySetted() {
		return (StringUtils.isNotBlank(orderBy) && StringUtils.isNotBlank(orderDir));
	}

	/**
	 * 是否默认计算总记录数.
	 */
	public boolean isCountTotal() {
		return countTotal;
	}

	/**
	 * 设置是否默认计算总记录数.
	 */
	public void setCountTotal(boolean countTotal) {
		this.countTotal = countTotal;
	}

	/**
	 * 根据pageNo和pageSize计算当前页第一条记录在总结果集中的位置, 序号从0开始.
	 */
	public int getOffset() {
		return ((pageNo - 1) * pageSize);
	}

	/**
	 * 获得页内的记录列表.
	 */
	public List<T> getResult() {
		return result;
	}

	/**
	 * 设置页内的记录列表.
	 */
	public void setResult(final List<T> result) {
		if (result instanceof com.github.pagehelper.Page) {
			com.github.pagehelper.Page<T> pageList = (com.github.pagehelper.Page<T>)result;
            this.totalCount = pageList.getTotal();
            this.result = pageList.getResult();
		} else {
			this.result = result;
		}
	}

	/**
	 * 获得总记录数, 默认值为-1.
	 */
	public long getTotalCount() {
		return totalCount;
	}

	/**
	 * 设置总记录数.
	 */
	public void setTotalCount(final long totalCount) {
		this.totalCount = totalCount;
	}

	/**
	 * 实现Iterable接口, 可以for(Object item : page)遍历使用
	 */
	@Override
	public Iterator<T> iterator() {
		return result.iterator();
	}

	/**
	 * 根据pageSize与totalItems计算总页数.
	 */
	public int getTotalPages() {
		return (int) Math.ceil((double) totalCount / (double) getPageSize());

	}

	/**
	 * 是否还有下一页.
	 */
	public boolean hasNextPage() {
		return (getPageNo() + 1 <= getTotalPages());
	}

	/**
	 * 是否最后一页.
	 */
	public boolean isLastPage() {
		return !hasNextPage();
	}

	/**
	 * 取得下页的页号, 序号从1开始. 当前页为尾页时仍返回尾页序号.
	 */
	public int getNextPage() {
		if (hasNextPage()) {
			return getPageNo() + 1;
		} else {
			return getPageNo();
		}
	}

	/**
	 * 是否还有上一页.
	 */
	public boolean hasPrePage() {
		return (getPageNo() > 1);
	}

	/**
	 * 是否第一页.
	 */
	public boolean isFirstPage() {
		return !hasPrePage();
	}

	/**
	 * 取得上页的页号, 序号从1开始. 当前页为首页时返回首页序号.
	 */
	public int getPrePage() {
		if (hasPrePage()) {
			return getPageNo() - 1;
		} else {
			return getPageNo();
		}
	}

	/**
	 * 计算以当前页为中心的页面列表,如"首页,23,24,25,26,27,末页" //@param count 需要计算的列表大小
	 * 
	 * @return pageNo列表
	 */
	public List<Integer> getSlider() {
		int count = 5;
		int halfSize = count / 2;
		int totalPage = getTotalPages();

		int startPageNo = Math.max(getPageNo() - halfSize, 1);
		int endPageNo = Math.min(startPageNo + count - 1, totalPage);

		if (endPageNo - startPageNo < count) {
			startPageNo = Math.max(endPageNo - count, 1);
		}

		List<Integer> result = Lists.newArrayList();
		for (int i = startPageNo; i <= endPageNo; i++) {
			result.add(i);
		}
		return result;
	}

	public static class Sort {
		public static final String ASC = "asc";
		public static final String DESC = "desc";

		private final String property;
		private final String dir;

		public Sort(String property, String dir) {
			this.property = property;
			this.dir = dir;
		}

		public String getProperty() {
			return property;
		}

		public String getDir() {
			return dir;
		}
		
	}
	
	public String getJsonResult() throws JsonProcessingException{
		ObjectMapper om = new ObjectMapper();
		return om.writeValueAsString(getResult());
	}
}
