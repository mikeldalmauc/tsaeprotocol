/*
* Copyright (c) Joan-Manuel Marques 2013. All rights reserved.
* DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
*
* This file is part of the practical assignment of Distributed Systems course.
*
* This code is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This code is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with this code.  If not, see <http://www.gnu.org/licenses/>.
*/

package recipes_service.tsae.data_structures;

import java.io.Serializable;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

//LSim logging system imports sgeag@2017
import edu.uoc.dpcs.lsim.LSimFactory;
import edu.uoc.dpcs.lsim.logger.LoggerManager.Level;
import lsim.worker.LSimWorker;
import recipes_service.data.Operation;

/**
 * @author Joan-Manuel Marques, Daniel Lázaro Iglesias
 * December 2012
 *
 */
public class Log implements Serializable{
	// Needed for the logging system sgeag@2017
	private transient LSimWorker lsim = LSimFactory.getWorkerInstance();

	private static final long serialVersionUID = -4864990265268259700L;
	/**
	 * This class implements a log, that stores the operations
	 * received  by a client.
	 * They are stored in a ConcurrentHashMap (a hash table),
	 * that stores a list of operations for each member of 
	 * the group.
	 */
	private ConcurrentHashMap<String, List<Operation>> log= new ConcurrentHashMap<String, List<Operation>>();  

	public Log(List<String> participants){
		// create an empty log
		for (Iterator<String> it = participants.iterator(); it.hasNext(); ){
			log.put(it.next(), new Vector<Operation>());
		}
	}

	/**
	 * inserts an operation into the log. Operations are 
	 * inserted in order. If the last operation for 
	 * the user is not the previous operation than the one 
	 * being inserted, the insertion will fail.
	 * 
	 * @param op
	 * @return true if op is inserted, false otherwise.
	 */
	public boolean add(Operation op){
		lsim.log(Level.TRACE, "Inserting into Log the operation: "+op);
		
		boolean result=false;
		
		//Obtenim el hostID referent a l'operaciÃ³ op		
		String host = op.getTimestamp().getHostid();		
		//Obtenim la llista d'operacions del Host
		List<Operation> opList = log.get(host);		
		//Comprovem que l'operaciÃ³ insertada presenta un timeStamp superior a l'operaciÃ³ anterior registrada per al Host
		if (!opList.isEmpty()){
			if (opList.get(opList.size()-1).getTimestamp().compare(op.getTimestamp())<0){
				log.get(host).add(op);
				result=true;
			}
		}
		else{
			//Si s'ha verificat que tot OK, llavors s'afegeix l'operaciÃ³ a la llista d'operacions del host
			log.get(host).add(op);
			result=true;
		}
		
		return(result);
	}
	
	/**
	 * Checks the received summary (sum) and determines the operations
	 * contained in the log that have not been seen by
	 * the proprietary of the summary.
	 * Returns them in an ordered list.
	 * @param sum
	 * @return list of operations
	 */
	public List<Operation> listNewer(TimestampVector sum){
		List<Operation> newOperations = new Vector<Operation>();
		
		//Recorrem la llista logs, per a introduir les noves operacions del log que no estan al summary		
		for(Map.Entry<String, List<Operation>> logList : log.entrySet()){
			//Recorrem el summary
			for(Map.Entry<String, Timestamp> sumList : sum.getTimestampVector().entrySet()){
				//Comprobem que el host del log sigui el mateix que el host del summary
				if (sumList.getKey().equals(logList.getKey())){
					//Recorrem totes les operacions del host emmagatzemadaes al log. Si alguna d'elles es superior a la que hi ha al summary, llavors l'afegim a la
					//llista de noves operacions
					for (Operation operation : logList.getValue()){
						if (operation.getTimestamp().compare(sumList.getValue())>0){
							newOperations.add(operation);
						}
					}
				}
			}
			
		}		
		return newOperations;		

	}
	
	/**
	 * Removes from the log the operations that have
	 * been acknowledged by all the members
	 * of the group, according to the provided
	 * ackSummary. 
	 * @param ack: ackSummary.
	 */
	public void purgeLog(TimestampMatrix ack){
	}

	/**
	 * equals
	 */
	@Override
	public boolean equals(Object obj) {
		
		Log logObj = (Log) obj;
		if (logObj.log.toString().equals(log.toString())){
			return true;
		}
		else{
			return false;
			
		}	
	}

	/**
	 * toString
	 */
	@Override
	public synchronized String toString() {
		String name="";
		for(Enumeration<List<Operation>> en=log.elements();
		en.hasMoreElements(); ){
		List<Operation> sublog=en.nextElement();
		for(ListIterator<Operation> en2=sublog.listIterator(); en2.hasNext();){
			name+=en2.next().toString()+"\n";
		}
	}
		
		return name;
	}
}