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
import java.util.Date;

import proj.zoie.api.indexing.DefaultOptimizeScheduler;

public class ZoieOptimizeSchedulerAdmin implements ZoieOptimizeSchedulerAdminMBean {
	private DefaultOptimizeScheduler _optimizeScheduler;
	
	public ZoieOptimizeSchedulerAdmin(DefaultOptimizeScheduler optimizeScheduler){
		_optimizeScheduler = optimizeScheduler;
	}
	
	public long getOptimizationDuration() {
		return _optimizeScheduler.getOptimizeDuration();
	}

	public void setOptimizationDuration(long duration) {
		_optimizeScheduler.setOptimizeDuration(duration);
	}
	

	public void setDateToStartOptimize(Date optimizeStartDate){
		_optimizeScheduler.setDateToStartOptimize(optimizeStartDate);
	}
	
	public Date getDateToStartOptimize(){
		return _optimizeScheduler.getDateToStartOptimize();
	}
}
