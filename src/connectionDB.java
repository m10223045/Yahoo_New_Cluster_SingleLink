import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;


public class connectionDB {
	private Connection con;
	private PreparedStatement pst;
	//con = DriverManager.getConnection("jdbc:mysql://localhost:3306/news_yahoo", "root", "idsl");
	
	public connectionDB(String url,String id,String pwd) throws SQLException{
		pst=null;
		con=DriverManager.getConnection(url, id, pwd);
	}

}
