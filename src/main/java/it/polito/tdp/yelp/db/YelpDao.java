package it.polito.tdp.yelp.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import it.polito.tdp.yelp.model.Business;
import it.polito.tdp.yelp.model.Review;
import it.polito.tdp.yelp.model.User;

public class YelpDao {

	public void getAllBusiness(Map<String, Business> businessMap){
		String sql = "SELECT * FROM Business";
		Connection conn = DBConnect.getConnection();

		try {
			PreparedStatement st = conn.prepareStatement(sql);
			ResultSet res = st.executeQuery();
			while (res.next()) {
				String id = res.getString("business_id");
				if(!businessMap.containsKey(id)) {
					Business business = new Business(id, 
							res.getString("full_address"),
							res.getString("active"),
							res.getString("categories"),
							res.getString("city"),
							res.getInt("review_count"),
							res.getString("business_name"),
							res.getString("neighborhoods"),
							res.getDouble("latitude"),
							res.getDouble("longitude"),
							res.getString("state"),
							res.getDouble("stars"));
					businessMap.put(id, business);
				}
			}
			res.close();
			st.close();
			conn.close();
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public List<Review> getAllReviews(){
		String sql = "SELECT * FROM Reviews";
		List<Review> result = new ArrayList<Review>();
		Connection conn = DBConnect.getConnection();

		try {
			PreparedStatement st = conn.prepareStatement(sql);
			ResultSet res = st.executeQuery();
			while (res.next()) {

				Review review = new Review(res.getString("review_id"), 
						res.getString("business_id"),
						res.getString("user_id"),
						res.getDouble("stars"),
						res.getDate("review_date").toLocalDate(),
						res.getInt("votes_funny"),
						res.getInt("votes_useful"),
						res.getInt("votes_cool"),
						res.getString("review_text"));
				result.add(review);
			}
			res.close();
			st.close();
			conn.close();
			return result;
			
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public void getAllUsers(Map<String, User> userMap){
		String sql = "SELECT * FROM Users";
		
		Connection conn = DBConnect.getConnection();

		try {
			PreparedStatement st = conn.prepareStatement(sql);
			ResultSet res = st.executeQuery();
			while (res.next()) {
				String id = res.getString("user_id");
				if(!userMap.containsKey(id)) {
					userMap.put(id, new User(id,
						res.getInt("votes_funny"),
						res.getInt("votes_useful"),
						res.getInt("votes_cool"),
						res.getString("name"),
						res.getDouble("average_stars"),
						res.getInt("review_count")));
				}
			}
			res.close();
			st.close();
			conn.close();
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public List<User> getUsersWithRewiewsMaggioriDi(int N, Map<String, User> userMap){
		String sql = "SELECT user_id "
				+ "FROM reviews "
				+ "GROUP BY user_id "
				+ "HAVING COUNT(review_id) >= ?";
		List<User> result = new ArrayList<User>();
		Connection conn = DBConnect.getConnection();

		try {
			PreparedStatement st = conn.prepareStatement(sql);
			st.setInt(1, N);
			ResultSet res = st.executeQuery();
			while (res.next()) {
				User user = userMap.get(res.getString("user_id"));
				if(user != null)
					result.add(user);
			}
			res.close();
			st.close();
			conn.close();
			return result;
			
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}

	public void getBusinessByUser(Map<String, User> userMap, Map<String, Business> businessMap,
			Map<User, List<Business>> businessByUser, int N) {
		String sql = "SELECT business_id, user_id "
				+ "FROM reviews r1 "
				+ "WHERE r1.user_id IN( "
				+ "SELECT user_id"
				+ "FROM reviews "
				+ "GROUP BY user_id "
				+ "HAVING COUNT(*) >= ?)";
		Connection conn = DBConnect.getConnection();

		try {
			PreparedStatement st = conn.prepareStatement(sql);
			st.setInt(1, N);
			ResultSet res = st.executeQuery();
			while (res.next()) {
				User user = userMap.get(res.getString("user_id"));
				Business business = businessMap.get(res.getString("business_id"));
				businessByUser.get(user).add(business);
			}
			res.close();
			st.close();
			conn.close();
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}
	
	public int calcolaSimilarita(User u1, User u2, int anno) {
		String sql = "SELECT COUNT(*) AS similarita "
				+ "FROM reviews r1, reviews r2 "
				+ "WHERE r1.business_id = r2.business_id "
				+ "AND r1.user_id = ? "
				+ "AND r2.user_id = ? "
				+ "AND YEAR(r1.review_date) = ? "
				+ "AND YEAR(r2.review_date) = ? " ;
		
		Connection conn = DBConnect.getConnection() ;
		try {
			PreparedStatement st = conn.prepareStatement(sql) ;
			st.setString(1, u1.getUserId());
			st.setString(2, u2.getUserId());
			st.setInt(3, anno);
			st.setInt(4, anno);
			
			ResultSet res = st.executeQuery() ;
			
			res.first();
			int similarita = res.getInt("similarita");
			conn.close();
			return similarita ;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}
	}
	
}
