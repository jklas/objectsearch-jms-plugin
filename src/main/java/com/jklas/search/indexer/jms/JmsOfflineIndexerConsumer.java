package com.jklas.search.indexer.jms;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;

import com.jklas.search.exception.IndexObjectException;
import com.jklas.search.indexer.IndexerService;

public class JmsOfflineIndexerConsumer implements ExceptionListener, Runnable {

	private final IndexerService indexerService;
	private final boolean indexIncomingObjects;
	
	private boolean running;
	private Session session;
	private Destination destination;
	private MessageProducer replyProducer;	
	private String subject = "SEARCH.ALL_INDEXES";	
	private boolean transacted;
	private boolean durable;
	private String clientId;
	private int ackMode = Session.AUTO_ACKNOWLEDGE;
	private ActiveMQConnectionFactory connectionFactory;
	private int receivedCount;
	private boolean shutdown = false;
	private Connection connection = null;
	private MessageConsumer consumer = null;
	private int countToStop;
	
	public JmsOfflineIndexerConsumer(String name, ActiveMQConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
		this.indexerService = null;
		this.indexIncomingObjects = false;
	}
	
	public JmsOfflineIndexerConsumer(String name, ActiveMQConnectionFactory connectionFactory, IndexerService indexerService) {
		this.connectionFactory = connectionFactory;
		this.indexerService = indexerService;
		this.indexIncomingObjects = true;
	}


	@Override
	public void run() {
		try {
			startup();
		} catch (JMSException e) {			
			e.printStackTrace();
			shutdown = true;			
		}

		try{
			while(!shutdown ) {
				try {
					read();					
				} catch (JMSException e) {					
					shutdown = true;
					e.printStackTrace();
				}
			}
		} finally {
			try {
				if(consumer!=null) consumer.close();

				if(session!=null) session.close();

				if(connection!=null) connection.close();
			} catch (JMSException ignore) {
				ignore.printStackTrace();
			}				
		}
	}


	protected void startup() throws JMSException {
		running = true;

		this.connection = connectionFactory.createConnection();
		if (durable && clientId != null && clientId.length() > 0 && !"null".equals(clientId)) {
			connection.setClientID(clientId);
		}

		connection.setExceptionListener(this);
		connection.start();

		session = connection.createSession(transacted, ackMode);

		destination = session.createQueue(subject);

		replyProducer = session.createProducer(null);
		replyProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

		consumer = session.createConsumer(destination);
	}

	private void log(String x) {
		//		System.out.println(x);
	}


	public synchronized void onException(JMSException ex) {
		log("JMS Exception occured.  Shutting down client.");
		running = false;
	}

	synchronized boolean isRunning() {
		return running;
	}

	private void read() throws JMSException {
		if(receivedCount >= this.countToStop) {
			shutdown = true;
			return;
		}

		Message message = consumer.receive();

		this.receivedCount ++;

		if(indexIncomingObjects) {			
			if (message instanceof ObjectMessage) {
				ObjectMessage objMsg = (ObjectMessage)message;
				
				JmsIndexPDU pdu = (JmsIndexPDU )objMsg.getObject();
				
				try {
					pdu.getAction().execute(indexerService, pdu.getIndexObjectDto());
				} catch (IndexObjectException e) {
					log(e);
				}
			}       
		}
		

		if (transacted) {
			session.commit();
		} else if (ackMode == Session.CLIENT_ACKNOWLEDGE) {
			message.acknowledge();
		}
	}

	private void log(IndexObjectException e) {
		// TODO Auto-generated method stub
		
	}

	public void setAckMode(String ackMode) {
		if ("CLIENT_ACKNOWLEDGE".equals(ackMode)) {
			this.ackMode = Session.CLIENT_ACKNOWLEDGE;
		}
		if ("AUTO_ACKNOWLEDGE".equals(ackMode)) {
			this.ackMode = Session.AUTO_ACKNOWLEDGE;
		}
		if ("DUPS_OK_ACKNOWLEDGE".equals(ackMode)) {
			this.ackMode = Session.DUPS_OK_ACKNOWLEDGE;
		}
		if ("SESSION_TRANSACTED".equals(ackMode)) {
			this.ackMode = Session.SESSION_TRANSACTED;
		}
	}

	public void setClientId(String clientID) {
		this.clientId = clientID;
	}

	public void setDurable(boolean durable) {
		this.durable = durable;
	}

	public void setSubject(String subject) {
		this.subject = subject;
	}

	public void setTransacted(boolean transacted) {
		this.transacted = transacted;
	}

	public int getReceivedCount() {		
		return receivedCount;
	}


	public void stopWhenReceivedCountReaches(int maxMessagesToRead) {
		this.countToStop = maxMessagesToRead;
	}
}
