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
