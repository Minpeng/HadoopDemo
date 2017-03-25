package com.hbase.demo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 利用phoenix 来查询hbase
 * 
 * @author pengm
 *
 */
public class ConnectHbase {
	/**
	 * 
	 * 建立连接
	 * 
	 * @return
	 */
	public static Connection getConnection() {
		Connection connection = null;
		String driver = "org.apache.phoenix.jdbc.PhoenixDriver";
		String url = "jdbc:phoenix:ui-test1,ui-test2,ui-test6";
		try {
			Class.forName( driver );
		}
		catch( ClassNotFoundException e ) {
			// TODO: handle exception
			e.printStackTrace();
		}
		if( connection == null ) {
			try {
				connection = DriverManager.getConnection( url );
			}
			catch( Exception e ) {
				// TODO: handle exception
				e.printStackTrace();
			}

		}
		return connection;

	}

	public static void main( String[] args ) throws SQLException {

		Connection conn = getConnection();

		PreparedStatement preparedStatement;

		String sql = "select \"key\" as keyId, \"DownCounts\"  as downCounts ,\"Date\" as dateString , \"MarketName\" as market  from  \"u:T_INFO\" "
		        + " where \"key\" between ? and ? and \"MarketName\" =? ";
		System.out.println( sql );
		long time = System.currentTimeMillis();
		preparedStatement = conn.prepareStatement( sql );
		preparedStatement.setString( 1, "d92fa482f4312f029bcbe7d44700b66a+20170101+" );
		preparedStatement.setString( 2, "d92fa482f4312f029bcbe7d44700b66a+20170219," );
		preparedStatement.setString( 3, "M3" );
		// preparedStatement.setInt( parameterIndex, x );

		ResultSet rs = preparedStatement.executeQuery();

		while( rs.next() ) {
			String keyId = rs.getString( "keyId" );
			String dateString = rs.getString( "dateString" );
			String market = rs.getString( "market" );
			Long downCounts = rs.getLong( "downcounts" );
			System.out.println( keyId + " " + dateString + " " + market + " " + downCounts );
		}
		long timeUsed = System.currentTimeMillis() - time;
		System.out.println( "time " + timeUsed + "mm" );
		// 关闭连接
		rs.close();
		preparedStatement.close();
		conn.close();
	}

}
