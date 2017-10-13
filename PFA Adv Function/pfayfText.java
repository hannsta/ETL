package pfaYF;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.hof.mi.interfaces.AnalyticalFunction;
import com.hof.mi.interfaces.UserInputParameters;
import com.hof.util.YFLogger;
import com.opendatagroup.antinous.pfainterface.PFAEngineFactory;
import com.opendatagroup.hadrian.jvmcompiler.PFAEngine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;


@SuppressWarnings("serial")
public class pfayfText
  extends AnalyticalFunction
{
  private Integer numfields = 0;
  private Object[] result = null;
  private boolean loaded = false;
  public Integer type = null;
  private static final YFLogger log = YFLogger.getLogger(pfayfText.class.getName());

  private String descText= "Select a PFA File and click Load";
  private UserInputParameters.Parameter loadswitch = new UserInputParameters.Parameter();
  private UserInputParameters.Parameter yaml = new UserInputParameters.Parameter();
  private List<com.hof.mi.interfaces.UserInputParameters.Parameter> plist = new ArrayList<com.hof.mi.interfaces.UserInputParameters.Parameter>();
  @SuppressWarnings("unused")
  public boolean acceptsNativeType(int i) {
		return i==1;	
	}
  public Object applyAnalyticFunction(int j, Object val)
    throws Exception
  {
	if (result==null){
		return null;
	}
    return this.result[j];
   
  }
  public String getCategory()
  {
    return "Plugins";
  }
  public String getColumnHeading(String arg0)
  {
    return "PFA Process";
  }
  public String getDescription()
  {
    return descText;
  }
  public String getName()
  {
    return "PFA Process (Numeric)";
  }
  public int getReturnType()
  {
	//Currently have to pick one, and numeric seems to be the most generally appropriate 
    return 1;
  }
  protected void setupParameters()
  {
    com.hof.mi.interfaces.UserInputParameters.Parameter parameter = 
      new UserInputParameters.Parameter();
    parameter.setUniqueKey("filename");
    parameter.setDisplayName("PFA File");
    parameter.setDescription("Complete path for PFA file");
	parameter.setDataType(UserInputParameters.TYPE_TEXT);
	parameter.setDisplayType(UserInputParameters.DISPLAY_TEXT_LONG);
	parameter.setDefaultValue(new String(""));
	addParameter(parameter);
    
    
	yaml.setUniqueKey("YAML");
	yaml.setDisplayName("PFA File Type");
	yaml.setDescription("");
	yaml.setDataType(2);
	yaml.setDisplayType(7);
	yaml.addOption("YAML", "YAML");
	yaml.addOption("JSON", "JSON");
	yaml.setDefaultValue("YAML");
    addParameter(yaml);
	
	loadswitch.setUniqueKey("Load");
	loadswitch.setDisplayName("Load PFA");
	loadswitch.setDescription("");
	loadswitch.setDataType(2);
	loadswitch.setDisplayType(7);
	loadswitch.addOption("No", "Reset");
	loadswitch.addOption("Yes", "Load");
	loadswitch.setDefaultValue("No");
    addParameter(loadswitch);

	//Create 15 (all initially set to not required)
	for (int i=1;i<25;i++){
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
  public void preAnalyticFunction(Object[] paramArrayOfObject) throws JsonGenerationException, JsonMappingException, IOException
  {

	  
	  try{
	  	String filepath = (String)getParameterValue("filename");
		String isyaml= (String)getParameterValue("YAML");
		PFAEngineFactory factory = new PFAEngineFactory();
		PFAEngine<Object, Object>  engine = null;
		
		String pfafile;
		try {
			pfafile = new String(Files.readAllBytes(Paths.get(filepath)));
			if (isyaml.equals("YAML")){
				engine = factory.engineFromYaml(pfafile);
			}else{
				engine = factory.engineFromJson(pfafile);	
			}	
		} catch (IOException e) {
			log.error("Cannot read specified PFA file");
			log.error(e.getMessage());
		}  
		Schema myschema =engine.inputType().schema();

		List<Object []> data = new ArrayList<>(15);
		Integer numf=1;
	    if (myschema.getType().toString().equals("RECORD")) numf= myschema.getFields().size();
		//Create table data matrix, and FieldName list
		for(int i=1; i<numf+1;i++){
	    	String keystr="p"+i;
	    	Object [] temp = (Object[]) getParameterValue(keystr);
	    	data.add(temp);
		}
	    List<Object> resultz= new ArrayList<>(data.get(0).length); 
		ArrayList<String> fieldnames = new ArrayList<String>();
	    if (myschema.getType().toString().equals("RECORD")){
			Iterator<Field> myit = myschema.getFields().iterator();
			while (myit.hasNext()){
				fieldnames.add(myit.next().name());
			}
	    }
		ArrayList<String>  rows = new ArrayList<String>();
	    for (int i=0; i<data.get(0).length;i++){
		    JsonObject jObj = new JsonObject();
		    
	    	for (int j=0; j<fieldnames.size();j++){
	    		Object[] row=(Object[]) data.get(j);
	    		jObj.add(fieldnames.get(j), new Gson().toJsonTree(row[i]));
	    	}
	    	if (fieldnames.size()==0){
	    		ObjectMapper mapper = new ObjectMapper();
	    		rows.add(mapper.writeValueAsString(data.get(0)[i]));
	    	}else{
	    		rows.add(jObj.toString());
	    	}
	    }
	    Iterator<String> itr = rows.iterator();
		engine.begin();
		Object inputDataStream = engine.jsonInputIterator(itr);
		while (itr.hasNext()){
			Object result = engine.action(((Iterator<Field>) inputDataStream).next());
			Double num = Double.valueOf(result.toString());
			resultz.add(num);
		}
		
		engine.end();
		
		this.result = resultz.toArray();
	  }catch (Exception e){
			log.error("Error retrieving result from PFA engine");
			log.error(e.getMessage());	 	  }
  }
  public boolean hasDependentParameters(String paramString)
  {
    if ("Load".equals(paramString)) {
      return true;
    }
    return false;
  }
  //Generates the user prompt options when the "Load" button is clicked
  public boolean isParameterRequired(String paramString){
	  //Try to load the specified PMML file, and generate additional user prompt labels
	  if (((String) getParameterValue("Load")).length() > 2 && this.loaded == false){
	    String filepath = (String)getParameterValue("filename");
		String isyaml= (String)getParameterValue("YAML");
		
		PFAEngineFactory factory = new PFAEngineFactory();
		PFAEngine<Object, Object>  engine = null;
		String pfafile;
		try {
			pfafile = new String(Files.readAllBytes(Paths.get(filepath)));
			if (isyaml.equals("YAML")){
				engine = factory.engineFromYaml(pfafile);
			}else{
				engine = factory.engineFromJson(pfafile);	
			}	
		} catch (IOException e) {
			log.error("Cannot read specified PFA file");
			log.error(e.getMessage());		
		} 
		
	  if (engine!=null){
		
		int fcount=0;

		Schema localSchema =engine.inputType().schema();
		if (!localSchema.getType().toString().equals("RECORD")){
			this.plist.get(fcount).setDisplayName(localSchema.getType().toString());
			fcount++;
		}else{
			Iterator<Field> fields = localSchema.getFields().iterator();
			while (fields.hasNext()){
				this.plist.get(fcount).setDisplayName(fields.next().name());
				fcount++;
			}
		}
		this.numfields = fcount;
		this.loaded=true;
	}
	}else{
		//Reset button, "unload" the pmmml which will hide all parameters then clear dropdown
		if (((String) getParameterValue("Load")).length() < 3){
			this.loaded=false;
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
	 //hide or display column prompts depending on number of input fields
	  for (int i=1;i<25;i++){
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
	  
}}
