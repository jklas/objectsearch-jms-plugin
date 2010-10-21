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
package com.jklas.search.indexer.jms;

import java.io.Serializable;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;

public class JmsOfflineIndexerProducer implements Runnable {

	private LinkedBlockingQueue<Serializable> sendQueue = new LinkedBlockingQueue<Serializable>();

	private Destination destination;
	private boolean verbose = true;
	private long timeToLive;
	private String subject = "SEARCH.ALL_INDEXES";
	private boolean transacted;
	private boolean persistent = false;

	private String producerName;

	private final ActiveMQConnectionFactory connectionFactory;
	private Session session;
	private MessageProducer producer;
	private boolean shutdown = false;

	private Connection connection;

	private int countToStop;

	private int sentCount = 0;

	public JmsOfflineIndexerProducer(String name, ActiveMQConnectionFactory connectionFactory) {
		this.producerName = name;
		this.connectionFactory = connectionFactory;
	}

	@Override
	public void run() {
		try {
			try {
				startup();
			} catch (JMSException e) {
				e.printStackTrace();
				shutdown = true;
			}

			while(!shutdown) {
				try {

					if(sentCount >= countToStop ) {
						shutdown = true;					
					} else {
						acceptAndSend();						
					}

				} catch (InterruptedException e) {

				}
			}
		} finally {
			try {					
				if(producer!=null) producer.close();
				if(session!=null) session.close();
				if(connection!=null) connection.close();
			} catch (JMSException ignore) {
				ignore.printStackTrace();
			}
		}

	}

	private void acceptAndSend() throws InterruptedException {
		Serializable objectToSend = sendQueue.take();
		send(objectToSend);
	}

	public void startup() throws JMSException {
		this.connection = null;
		
		this.connection = connectionFactory.createConnection();
		this.connection.start();

		this.session = connection.createSession(transacted, Session.AUTO_ACKNOWLEDGE);
		this.destination = session.createQueue(subject);

		// Create the producer.
		this.producer = session.createProducer(destination);
		
		if (persistent) {
			producer.setDeliveryMode(DeliveryMode.PERSISTENT);
		} else {
			producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
		}

		if (timeToLive != 0) {
			producer.setTimeToLive(timeToLive);
		}
		
	}

	private void log(String msg) {
//		System.out.println(msg);
	}

	public void send(Serializable objectToSend) {
		try {

			ObjectMessage message;
			message = session.createObjectMessage(objectToSend);

			if (verbose) {
				String msg = message.getObject().toString();
				if (msg.length() > 50) {
					msg = msg.substring(0, 50) + "...";
				}
				log("["+producerName+"]: " + msg);
			}

			producer.send(message);

			this.sentCount ++;


			if (transacted) {
				session.commit();
			}


		} catch (JMSException e) {
			throw new RuntimeException("JMS Exception, nesting...",e);
		}
	}

	public void setShutdown(boolean shutdown) {
		this.shutdown = shutdown;
	}

	public Queue<Serializable> getSendQueue() {
		return sendQueue;
	}

	public void stopWhenSentCountReaches(int count) {
		this.countToStop = count;
	}

	public int getUnsentMessageCount() {		
		return sendQueue.size();
	}

	public int getSentCount() {
		return sentCount;
	}
	
	public void setPersistent(boolean persistent) {
		this.persistent = persistent;
	}
	
	public boolean isPersistent() {
		return persistent;
	}
	
	public void setSubject(String subject) {
		this.subject = subject;
	}
	
	public String getSubject() {
		return subject;
	}
}
