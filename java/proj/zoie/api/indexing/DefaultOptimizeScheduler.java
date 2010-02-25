package proj.zoie.api.indexing;
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
import java.util.Calendar;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;

import proj.zoie.mbean.ZoieSystemAdminMBean;


public class DefaultOptimizeScheduler extends OptimizeScheduler {
	private static long DAY_IN_MILLIS = 1000L*60L*60L*24L;
	private static Logger logger = Logger.getLogger(DefaultOptimizeScheduler.class);
	
	private long _optimizeDuration;
	
    private volatile boolean _optimizeScheduled;
	private OptimizeType _optimizeType;
	
	private Timer _optimizeTimer;
	private TimerTask _currentOptimizationTimerTask;
	private Date _dateToStartOptimize;
	private final ZoieSystemAdminMBean _zoieAdmin;
	
	private class OptimizeTimerTask extends TimerTask{
		@Override
		public void run() {
		  _optimizeScheduled = true;
		}
	}
	
	// calculate to run next day at 1am
	private static Date calculateNextDay(){
		Calendar cal = Calendar.getInstance();
		int curHour = cal.get(Calendar.HOUR_OF_DAY);
		cal.set(Calendar.HOUR_OF_DAY, 1);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		if (curHour > 1){ 
		  cal.set(Calendar.DAY_OF_YEAR, cal.get(Calendar.DAY_OF_YEAR)+1);
		}
		return cal.getTime();
	}
	
	public DefaultOptimizeScheduler(ZoieSystemAdminMBean zoieAdmin){
		_optimizeDuration = DAY_IN_MILLIS;
		_optimizeScheduled = false;
		_dateToStartOptimize = calculateNextDay();
		_currentOptimizationTimerTask = new OptimizeTimerTask();
		_optimizeTimer = new Timer("index optimization timer",true);
		_optimizeTimer.scheduleAtFixedRate(_currentOptimizationTimerTask, _dateToStartOptimize, _optimizeDuration);
	    _optimizeType = OptimizeType.PARTIAL;
	    _zoieAdmin = zoieAdmin;
	}
	
	public long getOptimizeDuration(){
		return _optimizeDuration;
	}
	
	public synchronized void setOptimizeDuration(long optimizeDuration){
		if (_optimizeDuration!= optimizeDuration){
			_currentOptimizationTimerTask.cancel();
			_optimizeTimer.purge();
			_currentOptimizationTimerTask = new OptimizeTimerTask();
			_optimizeTimer.scheduleAtFixedRate(_currentOptimizationTimerTask, _dateToStartOptimize, optimizeDuration);
			_optimizeDuration = optimizeDuration;
		}
	}
	
	public synchronized void setDateToStartOptimize(Date optimizeStartDate){
		if (!_dateToStartOptimize.equals(optimizeStartDate)){
			_currentOptimizationTimerTask.cancel();
			_optimizeTimer.purge();
			_currentOptimizationTimerTask = new OptimizeTimerTask();
			_optimizeTimer.scheduleAtFixedRate(_currentOptimizationTimerTask, optimizeStartDate, _optimizeDuration);
			_dateToStartOptimize = optimizeStartDate;
		}
	}
	
	public Date getDateToStartOptimize(){
		return _dateToStartOptimize;
	}

    public OptimizeType getOptimizeType() {
      return _optimizeType;
    }
	
    public OptimizeType getScheduledOptimizeType() {
      return (_optimizeScheduled ? _optimizeType : OptimizeType.NONE);
    }
    
    public void finished() {
      _optimizeScheduled = false;
    }
}

