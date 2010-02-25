package proj.zoie.perf.client;

import org.springframework.remoting.httpinvoker.HttpInvokerProxyFactoryBean;

import proj.zoie.service.api.SearchRequest;
import proj.zoie.service.api.SearchResult;
import proj.zoie.service.api.ZoieSearchService;

public class ZoieClient {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		final HttpInvokerProxyFactoryBean factoryBean = new HttpInvokerProxyFactoryBean();

	    factoryBean.setServiceInterface(ZoieSearchService.class);
	    factoryBean.setServiceUrl("http://localhost:8888/zoie-perf/services/ZoieService");
	    factoryBean.afterPropertiesSet();

	    final ZoieSearchService svc = (ZoieSearchService) (factoryBean.getObject());
	    
	    SearchRequest req = new SearchRequest();
	   // req.setQuery("java");
	    SearchResult res = svc.search(req);

	    System.out.println("hits: "+res.getTotalHits()+" time: "+res.getTime());
	}

}
