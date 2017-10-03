
import com.hof.mi.interfaces.AnalyticalFunction;
import com.hof.mi.interfaces.UserInputParameters;

import hex.genmodel.MojoModel;
import hex.genmodel.easy.EasyPredictModelWrapper;
import hex.genmodel.easy.RowData;
import hex.genmodel.easy.exception.PredictException;
import hex.genmodel.easy.prediction.BinomialModelPrediction;
import hex.genmodel.easy.prediction.ClusteringModelPrediction;
import hex.genmodel.easy.prediction.MultinomialModelPrediction;
import hex.genmodel.easy.prediction.RegressionModelPrediction;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

@SuppressWarnings("serial")
public class H2OAdvancedFunctionText
  extends AnalyticalFunction
{
  private Integer numfields = 0;
  private Object[] result = null;
  private boolean loaded = false;
  private boolean loaded2 = false;
  public Integer type = null;
  private String descText= "Connect to your H2O server and generate predictions against the model of your choosing. Returns Text results.";
  private UserInputParameters.Parameter loadswitch = new UserInputParameters.Parameter();
  private UserInputParameters.Parameter connectswitch = new UserInputParameters.Parameter();
  private UserInputParameters.Parameter outputselect = new UserInputParameters.Parameter();
  private List<com.hof.mi.interfaces.UserInputParameters.Parameter> plist = new ArrayList<com.hof.mi.interfaces.UserInputParameters.Parameter>();
  @SuppressWarnings("unused")
  public boolean acceptsNativeType(int i) {
		return i==2;	
	}
  public Object applyAnalyticFunction(int j, Object val)
    throws Exception
  {
    return this.result[j];
   
  }

  public String getCategory()
  {
    return "Plugins";
  }
  public String getColumnHeading(String arg0)
  {
    return "H2O Predictions";
  }
  public String getDescription()
  {
    return descText;
  }
  public String getName()
  {
    return "H2O Predictions (Text)";
  }
  public int getReturnType()
  {
	//Currently have to pick one, and numeric seems to be the most generally appropriate 
    return 2;
  }
  protected void setupParameters()
  {
    com.hof.mi.interfaces.UserInputParameters.Parameter parameter = 
      new UserInputParameters.Parameter();
    parameter.setUniqueKey("url");
    parameter.setDisplayName("H2O URL");
    parameter.setDescription("URL for the H2O instance.");
	parameter.setDataType(UserInputParameters.TYPE_TEXT);
	parameter.setDisplayType(UserInputParameters.DISPLAY_TEXT_LONG);
	parameter.setDefaultValue(new String(""));
	addParameter(parameter);
	
	connectswitch.setUniqueKey("Connect");
	connectswitch.setDisplayName("Connect to H2O");
	connectswitch.setDescription("");
	connectswitch.setDataType(2);
	connectswitch.setDisplayType(7);
	connectswitch.addOption("No", "Reset");
	connectswitch.addOption("Yes", "Connect");
	connectswitch.setDefaultValue("No");
    addParameter(connectswitch);
    
    outputselect = new UserInputParameters.Parameter();
    outputselect.setUniqueKey("outputfield");
    outputselect.setDisplayName("Available Models");
    outputselect.setDescription("Select which model to run predictions against");
    outputselect.setDataType(2);
    outputselect.setDisplayType(6);
    addParameter(outputselect);

	loadswitch.setUniqueKey("Load");
	loadswitch.setDisplayName("Load Model");
	loadswitch.setDescription("");
	loadswitch.setDataType(2);
	loadswitch.setDisplayType(7);
	loadswitch.addOption("No", "Reset");
	loadswitch.addOption("Yes", "Load");
	loadswitch.setDefaultValue("No");
    addParameter(loadswitch);
    

    
	//Create 15 (all initially set to not required)
	for (int i=1;i<16;i++){
		String ukey = "p"+i;
		String pkey = "Parameter "+i;
	    parameter = new UserInputParameters.Parameter();
	    parameter.setUniqueKey(ukey);
	    parameter.setDisplayName(pkey);
	    parameter.setDescription("");
	    parameter.setDataType(100);
	    parameter.setDisplayType(6);
	    parameter.setAcceptsFieldType(6, true);
	    parameter.setAcceptsFieldType(3, true);
	    parameter.setAcceptsFieldType(1, true);
	    parameter.setAcceptsFieldType(2, true);
	    parameter.setAcceptsFieldType(4, true);
	    parameter.setAcceptsFieldType(5, true);
	    addParameter(parameter);
	    plist.add(parameter);
	}

  }
  //Runs the evaluator (for pre-loading see isParameterRequired)
  public void preAnalyticFunction(Object[] paramArrayOfObject)
  {
	  
	String baseurl = (String)getParameterValue("url");
    ArrayList<String> colnames = null;
    try {
		colnames = getColumns(baseurl, (String) getParameterValue("outputfield"));
	} catch (JSONException | IOException | URISyntaxException e1) {
	}
	List<Object []> data = new ArrayList<>(15);
		
	for(int i=0;i<colnames.size();i++){
		int ic=i+1;
	    String keystr="p"+ic;
	    Object [] temp = (Object[]) getParameterValue(keystr);
	    data.add(temp);
	}
	int rowcount= data.get(0).length;
	List<String> resultz= new ArrayList<>(rowcount); 
	  
	  
	String model_name=(String) getParameterValue("outputfield");

	//System.out.println(model.);
	  
	  
	EasyPredictModelWrapper model = null;
	try {
		model = getModelWrapper(baseurl, model_name);
	} catch (IOException | URISyntaxException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
    for (int i=0;i<rowcount;i++){
	    RowData row = new RowData();
    	for (int j=0; j<colnames.size();j++){
    	    Object[] col = data.get(j);
    	    row.put(colnames.get(j), col[i].toString());
    	    
    	}
        String category=model.getModelCategory().toString();
        Object result=null;
		try {
          if (category.equals("Regression")){
        	  RegressionModelPrediction prediction;

				prediction = model.predictRegression(row);

              result=prediction.value;
          }
          if (category.equals("Binomial")){
        	  BinomialModelPrediction prediction=model.predictBinomial(row);
        	  result=prediction.label;
          }
          if (category.equals("Clustering")){
        	  ClusteringModelPrediction prediction = model.predictClustering(row);
        	  result=prediction.cluster;
          }
          if (category.equals("Multinomial")){
        	  MultinomialModelPrediction prediction = model.predictMultinomial(row);
        	  result=prediction.label;
          }
          resultz.add(result.toString());
		} catch (PredictException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
          
      }   
	this.result = resultz.toArray();
  }
  public boolean hasDependentParameters(String paramString)
  {
    if ("Load".equals(paramString) || "Connect".equals(paramString)) {
      return true;
    }
    return false;
  }
  //Generates the user prompt options when the "Load" button is clicked
  public boolean isParameterRequired(String paramString){
	  //Try to load the specified PMML file, and generate additional user prompt labels
	  if (((String) getParameterValue("Connect")).length() > 2 && this.loaded==false){
		String url = (String)getParameterValue("url");
		ArrayList<String> modelnames = null;
		try {
			modelnames = getModels(url);
		} catch (IOException | JSONException | URISyntaxException e) {
			System.out.println("badurl");
			return false;
		}
	     for (int i=0;i<modelnames.size();i++){
	    	 System.out.println(modelnames.get(i));
	    	 this.outputselect.addOption(modelnames.get(i));
	     }
		 this.loaded=true; 
	  }
	  if (((String) getParameterValue("Load")).length() > 2 && this.loaded2 == false && this.loaded==true){
			String url = (String)getParameterValue("url");
		  	ArrayList<String> colnames = null;
			try {
				colnames = getColumns(url, (String) getParameterValue("outputfield"));
			  	this.numfields=colnames.size();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (colnames!=null){
		  	for (int i=0;i<colnames.size();i++){
		  		this.plist.get(i).setDisplayName(colnames.get(i).toString());
		  	}}
			this.loaded2=true;
	
	  }else{
		//Reset button, "unload" the pmmml which will hide all parameters then clear dropdown
		if (((String) getParameterValue("Load")).length() < 3){
			//this.outputselect.setOptions(null);
			this.loaded2=false;
		}
	}
	  //hide or display output field prompt 
	 if ("outputfield".equals(paramString)){
		 if (this.loaded==false){
			 
			return false;
		 }else{
			return true;
		 }
	 }
	 if ("Load".equals(paramString)){
		 if (this.loaded==false){
			 
			return false;
		 }else{
			return true;
		 }
	 }
	 //hide or display column prompts depending on number of input fields
	  for (int i=1;i<16;i++){
		  String ukey = "p"+i;
		  if (ukey.equals(paramString))
		    {
			  if (this.numfields>=i){
		        return true;
		      }
		      return false;
		    }
	  }

	  return true;
	  
}
  public static ArrayList<String> getModels(String baseurl) throws IOException, JSONException, URISyntaxException{
	  ArrayList<String> modelnames = new ArrayList<String>();
	  String request = baseurl+"/3/Models";
	  URL url = new URL(request);
	  URI uri = new URI(url.getProtocol(), url.getUserInfo(), url.getHost(), url.getPort(), url.getPath(), url.getQuery(), url.getRef());
	  url = uri.toURL();
	  HttpURLConnection conn = (HttpURLConnection) url.openConnection();           
	  conn.setInstanceFollowRedirects( false );
	  conn.setRequestMethod( "GET" );
	  BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));
	  String jsonstring = in.readLine();
	  JSONObject jsonArray = new JSONObject(jsonstring);
	 
	  List<String> acceptable = Arrays.asList("drf", "gbm", "glm", "kmeans", "xgboost");
	  for (int i=0;i<jsonArray.getJSONArray("models").length();i++){
		  JSONObject model=(JSONObject) jsonArray.getJSONArray("models").get(i);
		  String algo = model.get("algo").toString();
		  System.out.println("algorithm: "+algo);
		  if (acceptable.contains(algo)){
			  modelnames.add((String) model.getJSONObject("model_id").get("name"));	
		  }
	  }
	  return modelnames;
}
public static ArrayList<String> getColumns(String baseurl, String model_name) throws JSONException, UnsupportedEncodingException, IOException, URISyntaxException{
	  String request = baseurl+"/3/Models/"+model_name;
	  URL url = new URL(request);
	  URI uri = new URI(url.getProtocol(), url.getUserInfo(), url.getHost(), url.getPort(), url.getPath(), url.getQuery(), url.getRef());
	  url = uri.toURL();
	  HttpURLConnection conn = (HttpURLConnection) url.openConnection();           
	  conn.setInstanceFollowRedirects( false );
	  conn.setRequestMethod( "GET" );
	  BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));
	  String jsonstring = in.readLine();
	  jsonstring= StringUtils.replaceOnce(jsonstring,"algo","algo1");
	  jsonstring =StringUtils.replaceOnce(jsonstring,"algo_full_name","algo_full_name1");
	  jsonstring =StringUtils.replaceOnce(jsonstring,"model_id","model_id1");
	  JSONObject jsonArray = new JSONObject(jsonstring);
	  JSONObject frame=(JSONObject) jsonArray.getJSONArray("models").get(0);
	  String response="";
	  if (frame.get("response_column_name").toString()!=null){
		  response=frame.get("response_column_name").toString();
	  }
	  JSONArray columns= frame.getJSONObject("output").getJSONArray("names");
	  ArrayList<String> colnames = new ArrayList<String>();
	  for (int i=0;i<columns.length();i++){
		 String colname=columns.get(i).toString();
		 if (!colname.equals(response)){
			 colnames.add(colname);
		 }
	  }
	  return colnames;
} 
public static void copy(InputStream input, OutputStream output, int bufferSize) throws IOException {
    byte[] buf = new byte[bufferSize];
    int n = input.read(buf);
    while (n >= 0) {
      output.write(buf, 0, n);
      n = input.read(buf);
    }
    output.flush();
  }
public static EasyPredictModelWrapper getModelWrapper(String baseurl, String model_name) throws IOException, URISyntaxException{
	  String request = baseurl+"/3/Models/"+model_name+"/mojo";
	  URL url = new URL(request);
	  URI uri = new URI(url.getProtocol(), url.getUserInfo(), url.getHost(), url.getPort(), url.getPath(), url.getQuery(), url.getRef());
	  url = uri.toURL();	  HttpURLConnection conn = (HttpURLConnection) url.openConnection();           
	  conn.setInstanceFollowRedirects( false );
	  conn.setRequestMethod( "GET" );
	  InputStream in2 = conn.getInputStream();
	  File temp = File.createTempFile("tempfile", ".zip");
      temp.deleteOnExit();
	  FileOutputStream out = new FileOutputStream(temp);		  
      copy(in2, out, 1024);     
	  EasyPredictModelWrapper model = new EasyPredictModelWrapper(MojoModel.load(temp.getAbsolutePath()));
	  return model;
}
}
