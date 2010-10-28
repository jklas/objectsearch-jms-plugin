package com.jklas.search.indexer.jms;

import java.io.Serializable;

import com.jklas.search.index.dto.IndexObjectDto;
import com.jklas.search.indexer.IndexerAction;

public class JmsIndexPDU implements Serializable {

	private static final long serialVersionUID = 7404708979911227654L;	
	
	private IndexerAction action;
	
	private final IndexObjectDto indexObjectDto ;
		
	public JmsIndexPDU(IndexerAction action, IndexObjectDto indexObjectDto) {
		this.action = action;
		this.indexObjectDto = indexObjectDto;
	}
	
	public IndexerAction getAction() {
		return action;
	}
	
	public IndexObjectDto getIndexObjectDto() {
		return indexObjectDto;
	}

	
}
