package com.fluxtream.connectors.dao;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import com.fluxtream.TimeInterval;
import com.fluxtream.connectors.Connector;
import com.fluxtream.connectors.ObjectType;
import com.fluxtream.connectors.updaters.AbstractUpdater;
import com.fluxtream.domain.AbstractFacet;
import com.fluxtream.domain.ApiKey;
import com.fluxtream.domain.Updatable;
import com.fluxtream.services.ConnectorUpdateService;
import com.fluxtream.services.GuestService;
import com.fluxtream.utils.JPAUtils;

@Repository
public class JPAFacetDao implements FacetDao {

	@Autowired
	GuestService guestService;

	@Autowired
	ConnectorUpdateService connectorUpdateService;
	
	@PersistenceContext
	private EntityManager em;

	public JPAFacetDao() {}
	
	@Override
	public List<AbstractFacet> getFacetsBetween(Connector connector, long guestId, ObjectType objectType, TimeInterval timeInterval) {
		ArrayList<AbstractFacet> facets = new ArrayList<AbstractFacet>();
		
		if (objectType!=null) {
			String queryName = connector.getName().toLowerCase()
					+ "." + objectType.getName().toLowerCase()
					+ ".between";
			List<? extends AbstractFacet> found = JPAUtils.find(em, objectType.facetClass(), queryName, guestId, timeInterval.start, timeInterval.end);
			facets.addAll(found);
		} else {
			if (connector.objectTypes()!=null) {
				for (ObjectType type : connector.objectTypes()) {
					String queryName = connector.getName().toLowerCase()
							+ "." + type.getName().toLowerCase()
							+ ".between";
					facets.addAll(JPAUtils.find(em, type.facetClass(), queryName, guestId, timeInterval.start, timeInterval.end));
				}
			} else {
				String queryName = connector.getName().toLowerCase()
						+ ".between";
				facets.addAll(JPAUtils.find(em, connector.facetClass(), queryName, guestId, timeInterval.start, timeInterval.end));
			}
		}
		ApiKey apiKey = null;
		AbstractUpdater updater = null;
		for (AbstractFacet facet : facets) {
			if (facet instanceof Updatable) {
				if (apiKey==null) apiKey = guestService.getApiKey(guestId, connector);
				if (updater==null) updater = connectorUpdateService.getUpdater(connector);
				((Updatable)facet).update(updater, apiKey);
			}
		}
		return facets;
	}
	
	@Override
	public void deleteAllFacets(Connector connector, long guestId) {
		if (connector.objectTypes()==null) {
			String queryName = connector.getName().toLowerCase() + ".deleteAll";
			JPAUtils.execute(em, queryName, guestId);
		} else {
			for (ObjectType objectType : connector.objectTypes()) {
				String queryName = connector.getName().toLowerCase()
						+ "." + objectType.getName().toLowerCase()
						+ ".deleteAll";
				JPAUtils.execute(em, queryName, guestId);
			}
		}
	}

	@Override
	public void deleteAllFacets(Connector connector, ObjectType objectType,
			long guestId) {
		if (objectType==null) {
//			logger.warn(guestId, "trying to delete all facets with connector and objectType, but objectType is null \n" + stackTrace(new RuntimeException()));
			deleteAllFacets(connector, guestId);
		} else {
			String queryName = connector.getName().toLowerCase()
					+ "." + objectType.getName().toLowerCase()
					+ ".deleteAll";
			JPAUtils.execute(em, queryName, guestId);
		}
	}

	@Override
	@Transactional(readOnly=false)
	public void persist(Object o) {
		em.persist(o);
	}

	@Override
	@Transactional(readOnly=false)
	public void merge(Object o) {
		em.merge(o);
	}

}