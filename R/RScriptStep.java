package rstep;
import com.hof.mi.etl.ETLElement;
import com.hof.mi.etl.ETLException;
import com.hof.mi.etl.ETLStepCategory;
import com.hof.mi.etl.cache.ETLDataCache;
import com.hof.mi.etl.data.ETLStepBean;
import com.hof.mi.etl.data.ETLStepMetadataFieldBean;
import com.hof.mi.etl.runner.ETLStepResult;
import com.hof.mi.etl.step.AbstractETLCachedStep;
import com.hof.mi.etl.step.AbstractETLStep;
import com.hof.mi.etl.step.definition.ui.ETLStepConfigPanel;
import com.hof.mi.etl.step.definition.ui.ETLStepConfigSection;
import com.hof.mi.etl.step.definition.ui.ETLStepPanels;
import com.hof.parameters.GeneralPanelOptions;
import com.hof.parameters.InputType;
import com.hof.parameters.Parameter;
import com.hof.parameters.ParameterDisplayRule;
import com.hof.parameters.ParameterImpl;
import com.hof.parameters.ParameterPanelCollection;
import com.hof.parameters.ParameterValidation;
import com.hof.util.YFLogger;

import java.io.PrintStream;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.collections4.map.ListOrderedMap;
import org.json.JSONArray;
import org.json.JSONException;
import org.rosuda.REngine.REXP;
import org.rosuda.REngine.REXPDouble;
import org.rosuda.REngine.REXPFactor;
import org.rosuda.REngine.REXPLogical;
import org.rosuda.REngine.REXPString;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RserveException;


public class RScriptStep
  extends AbstractETLCachedStep
{
  boolean needsAdding=false;
  int addedfields=0;
  String Rerror = "";
  private static final YFLogger log = YFLogger.getLogger(RScriptStep.class.getName()); 
  public String getDefaultName()
  {
    return "R Script";
  }
  
  public String getDefaultDescription()
  {
    return "Run an R Script";
  }
  
  public void setupGeneratedFields()
  {
	  List<ETLStepMetadataFieldBean> fields = this.getDefaultMetadataFields();
	  if (fields.size()>0 && getStepOption("numTotalFields")!=null && getStepOption("return").equals("overwrite")){
		  int fcount=0;
		  for (ETLStepMetadataFieldBean field: fields){
			  if (fcount<Integer.valueOf(getStepOption("numTotalFields"))){
				  field.setFieldName("field"+Integer.toString(fcount));
				  field.setFieldType("TEXT");
				  field.setStepIncludeField(true);
			  }else{
				  field.setStepIncludeField(false);
			  }
			  fcount++;
		  }
	  }
	  if (fields.size()>0 && getStepOption("numExtraFields")!=null && getStepOption("return").equals("append")){
		  int j=0;
		  while (getStepOption("newField"+Integer.toString(j))!=null){
			  removeDefaultMetadataField(getStepOption("newField"+Integer.toString(j)));
			  j++;
		  }
		  for (int i=0;i<Integer.valueOf(getStepOption("numExtraFields"));i++){
			  if (fields.size()>0 && getStepOption("prediction")==null){
				  ETLStepMetadataFieldBean newField = fields.get(0).duplicate();
				  newField.setFieldName("newField"+Integer.toString(i));
				  newField.setFieldType("TEXT");
				  addNewGeneratedField(newField, "newField"+Integer.toString(i));
			  }
		  }
	  }
  }
  
  public Map<String, String> getDefaultInternalOptions()
  {
    return null;
  }
  
  public Map<String, String> getValidatedStepOptions()
  {
    return getStepOptions();
  }
  
  public Integer getMinInputSteps()
  {
    return Integer.valueOf(1);
  }
  
  public Integer getMaxInputSteps()
  {
    return Integer.valueOf(1);
  }
  
  public ETLStepCategory getStepCategory()
  {
    return ETLStepCategory.TRANSFORM;
  }
  
  public Collection<ETLException> validate()
  {
    return null;
  }
  
  protected void processEndRows()
    throws ETLException, InterruptedException
  {
	log.info("Initiating Rserve connection..");
	RConnection c = null;
	try {
		if (Boolean.valueOf(getStepOption("external"))){
			c = new RConnection(getStepOption("host"),Integer.valueOf(getStepOption("port")));
		}else{
			c = new RConnection();
		}
	} catch (RserveException e1) {
		e1.printStackTrace();
    	return;
	}
	log.info("Testing Rserve connection..");
	try {
		c.eval("1==1");
	} catch (RserveException e1) {
    	if (c!=null) c.close();
    	return;
	}
	log.info("Rserve connection established.");
	try{
	Map<String, Object[]> data = new HashMap();
    ETLDataCache dataCache = null;
    List<String> fieldUUIDS = new ArrayList();
    List<String> fieldNames = new ArrayList();

    for (String flowUUID : getInputFlowUuids())
    {
      dataCache = getDataCache(flowUUID);
      for (ETLStepMetadataFieldBean field: dataCache.getMetadataFields()){
    	  fieldUUIDS.add(field.getEtlStepMetadataFieldUUID());
    	  fieldNames.add(field.getFieldName());
      }
    }
    if (getStepOption("numExtraFields")!=null){
        for (int i=0;i<Integer.valueOf(getStepOption("numExtraFields"));i++){
        	fieldUUIDS.add(getStepOption("newField"+Integer.toString(i)));
        }
    }


    Iterator<Object[]> dataIterator = dataCache.iterator();
    ArrayList<ArrayList<Object>> asColumns = new ArrayList<>();
    int rowCount=0;
    while (dataIterator.hasNext())
    {
    	Object[] row = dataIterator.next();
    	for (int i=0;i<row.length;i++){
    		if (asColumns.size()<=i)	asColumns.add(new ArrayList<>());
    		asColumns.get(i).add(row[i]);		
    	}
    }
    for (int d=0;d<fieldNames.size();d++){
    	data.put(fieldNames.get(d), asColumns.get(d).toArray());
    }

	c.eval("yfData <- data.frame("+fieldNames.get(0)+"="+createArrayString(data.get(fieldNames.get(0)))+")");
	for (int i=1;i<fieldNames.size();i++){
		c.eval("yfData[\""+fieldNames.get(i)+"\"]="+createArrayString(data.get(fieldNames.get(i))));
	}
	c.eval("attach(yfData)");

	String filepath = getStepOption("filepath").replace("\\", "/");
	String RCommand = "try(source(\""+filepath+"\"),silent=TRUE)";
	REXP r = c.parseAndEval(RCommand);
    if (r.inherits("try-error")) {
    	Rerror = r.asString();
    	log.error(Rerror);
    	c.close();
    	throw new ETLException(ETLElement.STEP,getUuid(), null, Rerror, null);
    }
	int colLength  = c.eval("ncol(yfData)").asInteger();
	int rowLength  = c.eval("nrow(yfData)").asInteger();

	Object[][] outData = new Object[rowLength][colLength];
	for (int i=0;i<colLength;i++){
		
		REXP list= (REXP) c.eval("yfData["+Integer.toString(i+1)+"]").asList().elementAt(0);
		System.out.println(list.getClass().getName());
		if (list instanceof REXPDouble ){
			double[] doubles = list.asDoubles();

			for (int j=0;j<doubles.length;j++){
				outData[j][i]=doubles[j];
			}
		}
		if (list instanceof REXPString|| list instanceof REXPFactor || list instanceof REXPLogical){
			String[] strings = list.asStrings();
			for (int j=0;j<strings.length;j++){
				outData[j][i]=strings[j];
			}			
		}
		
	}
	for (Object[] row : outData){
        String str2 = getFirstOutputFlow();
        ETLStepResult stepResult = getFreshDataPacket(str2);        
        beginInternalTransmission(row, fieldUUIDS);
        endInternalTransmission(stepResult);
        emitData(stepResult);
	}
	c.close();
    }catch (Exception e){
    	if (c!=null) c.close();
    	e.printStackTrace();
    }
    
    
  }

  protected ParameterPanelCollection generatePanelCollection()
  {
    ETLStepConfigPanel regressionPanel = new ETLStepConfigPanel("RSTEPCONFIG", getETLStepBean().getStepName());
    
    
    RScriptSection rsection = new RScriptSection(this);
    regressionPanel.addSection(rsection);
    
    GeneralPanelOptions panelOptions = new GeneralPanelOptions();
    panelOptions.setSaveOnClose(true);
    panelOptions.setSaveButton(true);
    panelOptions.setSaveText("Apply");
    
    regressionPanel.setGeneralOptions(panelOptions);
    
    ETLStepPanels regressionPanelCollection = new ETLStepPanels();
    regressionPanelCollection.addPanel(regressionPanel);
    return regressionPanelCollection;
  }
	
	public static String createArrayString(Object[] column){
		JSONArray columnAsString;
		try {
			columnAsString = new JSONArray(column);
			String rlist = columnAsString.toString().replaceFirst("\\[", "c(");
			return replaceLast(rlist,"]", ")");
		} catch (JSONException e) {
			e.printStackTrace();
			return null;
		}
	}
	static String replaceLast(String string, String substring, String replacement)
	{
	  int index = string.lastIndexOf(substring);
	  if (index == -1)
	    return string;
	  return string.substring(0, index) + replacement
	          + string.substring(index+substring.length());
	}
	  public void parseData(Map<String, Object> paramMap)
	  {
		  for (String str1 : paramMap.keySet()) {
		      Object str2 = paramMap.get(str1);
		      paramMap.put(str1, str2.toString());
		
		  }
	  }
}