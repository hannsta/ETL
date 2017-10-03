package sentimentAnalysis;


import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.json.JSONObject;


import com.hof.data.EntityBean;
import com.hof.mi.data.Wire;
import com.hof.mi.etl.ETLException;
import com.hof.mi.etl.ETLStepCategory;
import com.hof.mi.etl.data.ETLStepMetadataFieldBean;
import com.hof.mi.etl.step.AbstractETLRowStep;
import com.hof.mi.etl.step.AbstractETLStep;
import com.hof.mi.etl.step.ETLStep;
import com.hof.parameters.ParameterPanelCollection;
import com.hof.util.UtilString;

public class GoogleSentimentStep extends AbstractETLRowStep {
	public int counter = 0;
	private String scoreUUID;
	private String magnitudeUUID;
	@Override
	public String getDefaultName() {
		return "Google Sentiment Analysis";
	}

	@Override
	public String getDefaultDescription() {
		return "Perform sentiment analysis on text documents using the Google Natural Language API";
	}

	@Override
	public ETLStepCategory getStepCategory() {
		return ETLStepCategory.TRANSFORM;
	}

	@Override
	public Collection<ETLException> validate() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected ParameterPanelCollection generatePanelCollection() {
		ParameterPanelCollection mypan = new GoogleSentimentPanelCollection(this);
		return mypan;
	}

	@Override
	public void setupGeneratedFields() {
		
		List<ETLStepMetadataFieldBean> defaultFields = getDefaultMetadataFields();
		if ((defaultFields.size() > 0) && (UtilString.isNullOrEmpty(getStepOption("TEXTFIELD")))){			
			ETLStepMetadataFieldBean field = defaultFields.get(0);
			
			ETLStepMetadataFieldBean scoreField = field.duplicate();
			
			String scoreUUID=UUID.randomUUID().toString();
			scoreField.setEtlStepMetadataFieldUUID(scoreUUID);
			scoreField.setLinkFieldUUID(scoreUUID);
			scoreField.setFieldType("NUMERIC");
			scoreField.setFieldName("Sentiment Score");
			scoreField.setEBChangeStat(EntityBean.EB_INSERT);
			addNewGeneratedField(scoreField, "scorefield");

			String magUUID=UUID.randomUUID().toString();
			ETLStepMetadataFieldBean magnitudeField = field.duplicate();
			magnitudeField.setEtlStepMetadataFieldUUID(magUUID);
			magnitudeField.setLinkFieldUUID(magUUID);
			magnitudeField.setFieldType("NUMERIC");
			magnitudeField.setFieldName("Sentiment Magnitude");
			magnitudeField.setEBChangeStat(EntityBean.EB_INSERT);
			addNewGeneratedField(magnitudeField, "magfield");
		}
	}

	@Override
	public Map<String, String> getDefaultInternalOptions() {
		return null;
	}

	@Override
	public Map<String, String> getValidatedStepOptions() {
		return getStepOptions();
	}

	@Override
	protected boolean processWireData(List<ETLStepMetadataFieldBean> fields) throws ETLException, InterruptedException {
		Wire<Object, String> score = getWireForField(getStepOption("scorefield"));
		Wire<Object, String> magnitude = getWireForField(getStepOption("magfield"));


		String textDocument=getWireForField(getStepOption("TEXTFIELD")).getValue().toString();
		textDocument=textDocument.replaceAll("\\\"", "");

	
		
		String APIKey= getStepOption("APIKEY");
		String requestString = "{\"encodingType\": \"UTF8\",\"document\": {\"type\": \"PLAIN_TEXT\",\"content\": \""+textDocument+"\"}}";
		try {
		    URL url = new URL("https://language.googleapis.com/v1/documents:analyzeSentiment?key="+APIKey);
		    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
		    connection.setRequestMethod("POST");
		    connection.setDoOutput(true);
		    connection.setRequestProperty("Content-Type", "application/json");
		    OutputStreamWriter out = new OutputStreamWriter(connection.getOutputStream());  
		    out.write(requestString);
		    out.flush();
		    out.close();

		    InputStream response = connection.getInputStream();
		    String theString = IOUtils.toString(response);
		    JSONObject responseJSON = new JSONObject(theString);
		    JSONObject document = (JSONObject) responseJSON.get("documentSentiment");
			magnitude.send(document.get("magnitude")) ;
			score.send(document.get("score")) ;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return true;
	}
	

	public void parseData(Map<String, Object> data) {
	}

}
