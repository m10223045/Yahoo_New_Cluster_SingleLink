import java.awt.List;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class news {
	public static void main(String[] args) throws SQLException {
		
		int all=0;
		
		HashMap<String, Integer> TermsDF = new HashMap();
		HashMap<Integer, HashMap<String,Integer>> Documents = new HashMap();
		HashMap<String,Double> TermsIDF = new HashMap();
		
		ArrayList<Integer> DocList = new ArrayList();
		ArrayList<ArrayList<Integer>> Clusters = new ArrayList();
		news news =new news();
				
		Connection con = null;
		PreparedStatement pst = null;
		con = DriverManager.getConnection("jdbc:mysql://localhost:3306/news_yahoo", "root", "idsl");
		//String sql = "Select * from 103_yahoo_news_policy_timewindows_testset3000";
		String sql = "Select * from 103_yahoo_news_policy_timewindows_complete limit 1000";
		Statement stmt = con.createStatement();
		ResultSet rs = stmt.executeQuery(sql);
				
		rs.next();
		while (rs.next()) {
			String[] terms = rs.getString("CKIPTerms").replaceAll("\\[\\(|\\)\\]", "").split("\\), \\(");
			all+=terms.length;
			Documents.put(rs.getInt(1), new HashMap());
			DocList.add(rs.getInt(1));
			
			for (int i = 0; i < terms.length; i++) {			
				String[] termArray = terms[i].split(",");
				String term = termArray[0];
				//int length = term.length();
				Documents.get(rs.getInt(1)).put(termArray[0], (int)Double.parseDouble(termArray[1]));	
				
				if(TermsDF.get(term)==null){
					TermsDF.put(term, 1);
				}else{
					int count=TermsDF.get(term);
					TermsDF.remove(term);
					TermsDF.put(term, count+1);
					//TermsDF.put(term, TermsDF.get(term)+1);
				}										
			}
		}
		
		
		//IDF
		for(String key : TermsDF.keySet()){
			double idf=Math.log10(Documents.size()/TermsDF.get(key));			
			TermsIDF.put(key, idf);						
		}	
		System.out.println("Terms size:"+TermsIDF.size());
		System.out.println("Terms all:"+all);
		
		//news.TF_IDF(Documents, TermsIDF);
		//news.SinglePassCluster(0.365, Clusters, DocList, Documents);
		
	}
	/**
	 * TF_IDF
	 * @param Documents
	 * @param TermsIDF
	 * @throws SQLException
	 */
	public void TF_IDF(HashMap<Integer, HashMap<String,Integer>> Documents,HashMap<String,Double> TermsIDF) throws SQLException{
		String sql="";
		Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/news_yahoo", "root", "idsl");
		PreparedStatement pst = null;
		
		for(String term : TermsIDF.keySet()){
			for(Integer doc : Documents.keySet()){
				if(Documents.get(doc).get(term)!=null){
					double tf = Documents.get(doc).get(term);
					double idf = TermsIDF.get(term);
					double tf_idf=((1+Math.log(tf))*idf);
					
					System.out.println(term+"   "+doc+" tf="+tf+"   idf="+idf+"   tf-idf="+tf_idf);
					sql="INSERT INTO tf_idf2(term,document,value) VALUES(?,?,?)";
					pst = con.prepareStatement(sql);
		        	pst.setString(1,term);
		        	pst.setInt(2,doc);
		        	pst.setDouble(3, tf_idf);
		        	pst.executeUpdate();					
				}				
			}			
		}
		
		pst.close();
		con.close();
	}
	
	/**
	 * similarity calculate
	 * @param doc
	 * @param Cluster
	 * @param Documents
	 * @return
	 */
	public double sim_cos(Integer doc,ArrayList<Integer> Cluster,HashMap<Integer, HashMap<String,Integer>> Documents){
		double upWjx=0.0;
		double upWjc=0.0;
		double uptotal=0.0;
		double downx=0.0;
		double downc=0.0;
		HashMap<String,Integer> docTermsTF = Documents.get(doc);
		
		for(String term : docTermsTF.keySet()){
			for(Integer docID : Cluster){							
				if(Documents.get(docID).get(term)!=null)
					upWjc=upWjc+Documents.get(docID).get(term);			
			}					
			upWjx=docTermsTF.get(term);			
			uptotal=uptotal+(upWjx*upWjc);			
			downx=downx+(upWjx*upWjx);			
			downc=downc+(upWjc*upWjc);					
		}			
		return uptotal/Math.sqrt(downx*downc);
	}
	/**
	 * SinglePassCluster
	 * @param Threshold
	 * @param Clusters
	 * @param DocList
	 * @param Documents
	 * @throws SQLException
	 */
	public void SinglePassCluster(double Threshold,ArrayList<ArrayList<Integer>> Clusters,ArrayList<Integer> DocList,HashMap<Integer, HashMap<String,Integer>> Documents) throws SQLException{
		Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/news_yahoo", "root", "idsl");
		PreparedStatement pst = null;
				
		Clusters.add(new ArrayList());
		Clusters.get(0).add(DocList.get(0));		
		for(int d=1;d<DocList.size();d++){
			System.out.println("D "+DocList.get(d));
			int maxc=0;
			double siml=0.0;
			double simmax=0.0;
			for(int C=0;C<Clusters.size();C++){
				siml=sim_cos(DocList.get(d),Clusters.get(C),Documents);
				if(siml>=simmax){
					simmax=siml;
					maxc=C;
				}
			}
			System.out.println("simile:"+simmax);
			if( simmax > Threshold ){
				System.out.println(DocList.get(d)+" add to "+maxc);
				Clusters.get(maxc).add(DocList.get(d));
			}else{
				System.out.println(DocList.get(d)+"Create to new");
				Clusters.add(new ArrayList());
				int size=Clusters.size();
				Clusters.get(size-1).add(DocList.get(d));
			}
		}
		
		System.out.println("--------cluster is "+Clusters.size()+"---------------");
		HashMap<Integer,Integer> size = new HashMap();
		int bb=0;
		for(ArrayList<Integer> al:Clusters){
			size.put(bb, al.size());
			System.out.println("-**********--"+bb);
			
			pst = null; /*	       
			for(int p=0;p<al.size();p++){ 			
				String sql="UPDATE 103_yahoo_news_policy_timewindows_complete SET cluster=? where id=?";
				pst = con.prepareStatement(sql);
	        	pst.setInt(1, bb);
	        	pst.setInt(2, al.get(p));
	        	pst.executeUpdate();
				
				System.out.println(al.get(p)+"  ");
			}
			bb++;
			System.out.println("");*/
		}
		
		for(Integer id:size.keySet()){
			System.out.println(id+","+size.get(id));
		}	
		con.close();
		pst.close();
	}
	
}//class
