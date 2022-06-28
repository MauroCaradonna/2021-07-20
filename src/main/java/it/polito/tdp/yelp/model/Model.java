package it.polito.tdp.yelp.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jgrapht.Graph;
import org.jgrapht.Graphs;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleWeightedGraph;

import it.polito.tdp.yelp.db.YelpDao;

public class Model {
	
	private Graph<User, DefaultWeightedEdge> grafo;
	private Map<String, User> userMap;
	private Map<String, Business> businessMap;
	private YelpDao dao;
	private Map<User, List<Business>> businessByUser;
	
	public Model() {
		this.userMap = new HashMap<String, User>();
		this.businessMap = new HashMap<String, Business>();
		this.dao = new YelpDao();
		this.dao.getAllUsers(this.userMap);
		this.dao.getAllBusiness(this.businessMap);
	}
	
	public List<User> getUsersWithRewiewsMaggioriDi(int N){
		return this.dao.getUsersWithRewiewsMaggioriDi(N, userMap);
	}
	
	public Graph<User, DefaultWeightedEdge> creaGrafo(int N, int anno){
		this.grafo = new SimpleWeightedGraph<User, DefaultWeightedEdge>(DefaultWeightedEdge.class);
		List<User> users = this.dao.getUsersWithRewiewsMaggioriDi(N, userMap);
		for(User uu : userMap.values()) {
			if(uu == null) {
				System.out.println("null: " + uu);
			}
		}
		Graphs.addAllVertices(this.grafo, users);
		this.businessByUser = new HashMap<User, List<Business>>();
		for(User user : this.grafo.vertexSet()) {
			this.businessByUser.put(user,new ArrayList<Business>());
		}
		for(int i = 0; i < users.size() - 1; i ++) {
			for(int j = 0; j < users.size(); j++) {
				if(users.get(i).getUserId().compareTo(users.get(j).getUserId())<0) {
					int peso = this.dao.calcolaSimilarita(users.get(i), users.get(j), anno);
					if(peso > 0)
						Graphs.addEdgeWithVertices(this.grafo, users.get(i), users.get(j), peso);
				}
			}
		}
		return this.grafo;
	}
	
}
