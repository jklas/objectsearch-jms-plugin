/**
 * Object Search Framework
 *
 * Copyright (C) 2010 Julian Klas
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */
package com.jklas.search.indexer;

import java.io.Serializable;

import junit.framework.Assert;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.BeforeClass;
import org.junit.Test;

import com.jklas.search.indexer.Utils;
import com.jklas.search.exception.IndexObjectException;
import com.jklas.search.exception.SearchEngineMappingException;
import com.jklas.search.index.dto.IndexObjectDto;
import com.jklas.search.index.memory.MemoryIndex;
import com.jklas.search.index.memory.MemoryIndexWriterFactory;
import com.jklas.search.indexer.DefaultIndexerService;
import com.jklas.search.indexer.IndexerService;
import com.jklas.search.indexer.jms.JmsOfflineIndexer;
import com.jklas.search.indexer.jms.JmsOfflineIndexerConsumer;
import com.jklas.search.indexer.jms.JmsOfflineIndexerProducer;
import com.jklas.search.indexer.pipeline.DefaultIndexingPipeline;

public class JmsOfflineIndexerTest {

	private static ActiveMQConnectionFactory connectionFactory = null; 
	
	@BeforeClass
	public static void cleanUpQueues() {
        connectionFactory = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");
	}
	
	@Test
	public void OneObjectsSentIsReceived() throws InterruptedException, IndexObjectException {
		
		int numberOfObjectsToSend = 1;
		Serializable[] entities = new Serializable[numberOfObjectsToSend];
		Serializable[] ids = new Serializable[numberOfObjectsToSend];
		
		for (int i = 0; i < entities.length; i++) {
			entities[i]= new Utils.SingleAttributeEntity(i,"Julian" +i);
			ids[i]=i;
		}
		
		sendEntitiesOverJms(entities, ids);
    }

	@Test
	public void TenObjectsSentAreReceived() throws InterruptedException, IndexObjectException {
		
		int numberOfObjectsToSend = 10;
		Serializable[] entities = new Serializable[numberOfObjectsToSend];
		Serializable[] ids = new Serializable[numberOfObjectsToSend];
		
		for (int i = 0; i < entities.length; i++) {
			entities[i]= new Utils.SingleAttributeEntity(i,"Julian" +i);
			ids[i]=i;
		}
		
		sendEntitiesOverJms(entities, ids);
    }
	
	@Test
	public void TenObjectsSentAreIndexed() throws InterruptedException, IndexObjectException, SearchEngineMappingException {
		
		int numberOfObjectsToSend = 10;
		Serializable[] entities = new Serializable[numberOfObjectsToSend];
		Serializable[] ids = new Serializable[numberOfObjectsToSend];
		
		
		for (int i = 0; i < entities.length; i++) {
			entities[i]= new Utils.SingleAttributeEntity(i,"Julian" +i);
			ids[i]=i;
		}
		Utils.configureAndMap(entities[0]);
		
		sendEntitiesOverJmsForIndexing(entities, ids,
				new DefaultIndexerService(
						new DefaultIndexingPipeline(),
						MemoryIndexWriterFactory.getInstance())
			);
		
		Assert.assertEquals(10, MemoryIndex.getDefaultIndex().getObjectCount());
    }
	
	private void sendEntitiesOverJms(Serializable[] entities, Serializable[] ids) throws IndexObjectException, InterruptedException {
		JmsOfflineIndexerProducer tom = new JmsOfflineIndexerProducer("Tom",connectionFactory);
    	JmsOfflineIndexerConsumer jerry = new JmsOfflineIndexerConsumer("Jerry",connectionFactory);
        
        JmsOfflineIndexer offlineIndexer = new JmsOfflineIndexer(tom);
                
        Thread tomThread = new Thread(tom);
        tomThread.setName("Tom");
		tom.stopWhenSentCountReaches(entities.length);
		tomThread.start();
        
		Thread jerryThread = new Thread(jerry);
		jerryThread.setName("Jerry");		
		jerry.stopWhenReceivedCountReaches(entities.length);
		jerryThread.start();
        
		for (int i = 0; i < entities.length; i++) {			
			offlineIndexer.create(new IndexObjectDto(entities[i],ids[i]));
		}
								
		tomThread.join();
		Assert.assertEquals(0, tom.getUnsentMessageCount());
		Assert.assertEquals(entities.length, tom.getSentCount());
		
		jerryThread.join();
				
		Assert.assertEquals(entities.length, jerry.getReceivedCount());
	}

	private void sendEntitiesOverJmsForIndexing(Serializable[] entities, Serializable[] ids, IndexerService indexerService) throws IndexObjectException, InterruptedException {
		JmsOfflineIndexerProducer tom = new JmsOfflineIndexerProducer("Tom",connectionFactory);
    	JmsOfflineIndexerConsumer jerry = new JmsOfflineIndexerConsumer("Jerry",connectionFactory,indexerService);
        
        JmsOfflineIndexer offlineIndexer = new JmsOfflineIndexer(tom);
                
        Thread tomThread = new Thread(tom);
        tomThread.setName("Tom");
		tom.stopWhenSentCountReaches(entities.length);
		tomThread.start();
        
		Thread jerryThread = new Thread(jerry);
		jerryThread.setName("Jerry");		
		jerry.stopWhenReceivedCountReaches(entities.length);
		jerryThread.start();
        
		for (int i = 0; i < entities.length; i++) {			
			offlineIndexer.create(new IndexObjectDto(entities[i],ids[i]));
		}
								
		tomThread.join();
		Assert.assertEquals(0, tom.getUnsentMessageCount());
		Assert.assertEquals(entities.length, tom.getSentCount());
		
		jerryThread.join();
				
		Assert.assertEquals(entities.length, jerry.getReceivedCount());
	}

	
}
