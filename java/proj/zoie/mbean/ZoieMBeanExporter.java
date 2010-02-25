package proj.zoie.mbean;
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import org.apache.log4j.Logger;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.jmx.export.MBeanExporter;
import org.springframework.remoting.httpinvoker.HttpInvokerProxyFactoryBean;

import proj.zoie.service.api.SearchRequest;
import proj.zoie.service.api.SearchResult;
import proj.zoie.service.api.ZoieSearchService;

public class ZoieMBeanExporter extends MBeanExporter implements
		BeanFactoryAware, InitializingBean, DisposableBean {
    private static Logger logger = Logger.getLogger(ZoieMBeanExporter.class);
    
	@Override
	protected void registerBeans()
	{
	  try 
	  {
	    super.registerBeans();
	  } 
	  catch(Exception ex) {
	    logger.error("Instance already exists, registering JMX bean failed: "+ex.getMessage(),ex);
	  }
    }
	
	
	
	public static void main(String[] args) throws Exception{
		HttpInvokerProxyFactoryBean factoryBean = new HttpInvokerProxyFactoryBean();
		factoryBean.setServiceUrl("http://localhost:8888/services/SearchService");
		factoryBean.setServiceInterface(ZoieSearchService.class);
		factoryBean.afterPropertiesSet();
		
		ZoieSearchService svc = (ZoieSearchService)(factoryBean.getHttpInvokerRequestExecutor());
		
		SearchResult res = svc.search(new SearchRequest());
		
		System.out.println(res.getTotalHits());
	}
}
