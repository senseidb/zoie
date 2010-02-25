package proj.zoie.admin.server.search;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;

import org.apache.log4j.Logger;
import org.springframework.remoting.httpinvoker.HttpInvokerProxyFactoryBean;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

import proj.zoie.admin.client.search.SearchHit;
import proj.zoie.admin.client.search.SearchRequest;
import proj.zoie.admin.client.search.SearchResult;
import proj.zoie.admin.client.search.SearchService;
import proj.zoie.api.ZoieException;
import proj.zoie.service.api.ZoieSearchService;

import com.google.gwt.user.server.rpc.RemoteServiceServlet;


@SuppressWarnings("serial")
public class SearchServiceServlet extends RemoteServiceServlet implements SearchService
{
  private static final Logger log = Logger.getLogger(SearchServiceServlet.class);
	private WebApplicationContext _appCtx;
	private ZoieSearchService _searchSvc;

	@Override
	public void init(ServletConfig config) throws ServletException
	{
		super.init(config);
		ServletContext ctx = config.getServletContext();
		_appCtx = WebApplicationContextUtils.getRequiredWebApplicationContext(ctx);
    final HttpInvokerProxyFactoryBean factoryBean = new HttpInvokerProxyFactoryBean();
    factoryBean.setServiceInterface(ZoieSearchService.class);
    factoryBean.setServiceUrl("http://localhost:8888/zoie-perf/services/SampleZoieService");
    factoryBean.afterPropertiesSet();
		_searchSvc = (ZoieSearchService) factoryBean.getObject();
	}
	  
	public SearchResult search(SearchRequest req)
	{
    proj.zoie.service.api.SearchResult result = null;
    try
    {
		  proj.zoie.service.api.SearchRequest searchRequest = new proj.zoie.service.api.SearchRequest();
		  searchRequest.setQuery(req.getQuery());
		  result = _searchSvc.search(searchRequest);
    } catch (ZoieException e)
    {
      log.error(e);
      result = null;
    }
    if ((result==null) || (result.getHits()==null))
    {
      SearchResult res = new SearchResult();
      res.setHits(new SearchHit[0]);
      return res;
    }
    SearchResult res = new SearchResult();
    proj.zoie.service.api.SearchHit[] oldHits = result.getHits();
    SearchHit[] hits = new SearchHit[result.getHits().length];
    for(int i=0; i < oldHits.length; i++)
    {
      hits[i] = new SearchHit();
      hits[i].setFields(oldHits[i].getFields());
      hits[i].setScore(oldHits[i].getScore());
    }
    res.setHits(hits);
    res.setTime(result.getTime());
    res.setTotalDocs(result.getTotalDocs());
    res.setTotalHits(result.getTotalHits());
    return res;
	}
}
