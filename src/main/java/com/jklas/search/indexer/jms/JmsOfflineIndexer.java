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

import java.util.List;

import com.jklas.search.exception.IndexObjectException;
import com.jklas.search.index.dto.IndexObjectDto;
import com.jklas.search.indexer.IndexerAction;
import com.jklas.search.indexer.IndexerService;
import com.jklas.search.util.SearchLibrary;

public class JmsOfflineIndexer implements IndexerService {

	private final JmsOfflineIndexerProducer producer;
		
	public JmsOfflineIndexer(JmsOfflineIndexerProducer producer) {
		this.producer = producer;
	}
	
	@Override
	public void create(IndexObjectDto indexObjectDto) throws IndexObjectException {
		producer.getSendQueue().add(new JmsIndexPDU(IndexerAction.CREATE, indexObjectDto));	
	}


	@Override
	public void createOrUpdate(IndexObjectDto indexObjectDto) throws IndexObjectException {
		producer.getSendQueue().add(new JmsIndexPDU(IndexerAction.CREATE_OR_UPDATE, indexObjectDto));		
	}


	@Override
	public void delete(IndexObjectDto indexObjectDto) throws IndexObjectException {
		producer.getSendQueue().add(new JmsIndexPDU(IndexerAction.DELETE, indexObjectDto));
	}


	@Override
	public void update(IndexObjectDto indexObjectDto) throws IndexObjectException {
		producer.getSendQueue().add(new JmsIndexPDU(IndexerAction.UPDATE, indexObjectDto));
	}
			
	@Override
	public void create(Object entity) throws IndexObjectException {
		producer.getSendQueue().add(new JmsIndexPDU(IndexerAction.CREATE, new IndexObjectDto(entity)));
	}

	@Override
	public void createOrUpdate(Object entity) throws IndexObjectException {
		producer.getSendQueue().add(new JmsIndexPDU(IndexerAction.CREATE_OR_UPDATE, new IndexObjectDto(entity)));
	}

	@Override
	public void delete(Object entity) throws IndexObjectException {
		producer.getSendQueue().add(new JmsIndexPDU(IndexerAction.DELETE, new IndexObjectDto(entity)));
	}

	@Override
	public void update(Object entity) throws IndexObjectException {
		producer.getSendQueue().add(new JmsIndexPDU(IndexerAction.UPDATE, new IndexObjectDto(entity)));
	}

	@Override
	public void bulkCreate(List<?> entities) throws IndexObjectException {		
		for (Object entity : entities) {
			create(entity);
		}
	}

	@Override
	public void bulkCreateOrUpdate(List<?> entities) throws IndexObjectException {
		for (Object entity : entities) {
			createOrUpdate(entity);
		}
	}

	@Override
	public void bulkDelete(List<?> entities) throws IndexObjectException {
		for (Object entity : entities) {
			delete(entity);
		}
	}

	@Override
	public void bulkDtoCreate(List<IndexObjectDto> indexObjectDto) throws IndexObjectException {
		bulkCreate(SearchLibrary.convertDtoListToEntityList(indexObjectDto));
	}

	@Override
	public void bulkDtoCreateOrUpdate(List<IndexObjectDto> indexObjectDto) throws IndexObjectException {
		bulkCreateOrUpdate(SearchLibrary.convertDtoListToEntityList(indexObjectDto));
	}

	@Override
	public void bulkDtoDelete(List<IndexObjectDto> indexObjectDto) throws IndexObjectException {
		bulkDelete(SearchLibrary.convertDtoListToEntityList(indexObjectDto));
	}

	@Override
	public void bulkDtoUpdate(List<IndexObjectDto> indexObjectDto) throws IndexObjectException {
		bulkUpdate(SearchLibrary.convertDtoListToEntityList(indexObjectDto));
	}

	@Override
	public void bulkUpdate(List<?> entities) throws IndexObjectException {
		for (Object entity : entities) {
			update(entity);
		}
	}

}
