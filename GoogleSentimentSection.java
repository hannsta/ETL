package sentimentAnalysis;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.hof.mi.etl.data.ETLStepMetadataFieldBean;
import com.hof.mi.etl.step.AbstractETLStep;
import com.hof.mi.etl.step.ETLStep;
import com.hof.parameters.GeneralPanelOptions;
import com.hof.parameters.InputType;
import com.hof.parameters.ListOptions;
import com.hof.parameters.Parameter;
import com.hof.parameters.ParameterDisplayRule;
import com.hof.parameters.ParameterImpl;
import com.hof.parameters.ParameterSection;

public class GoogleSentimentSection extends ParameterSection {

	private ETLStep step;
	Map<String, Object> data;
	public GoogleSentimentSection(ETLStep step) {
		this.step = step;
		data = new HashMap<>();

	}
	
	@Override
	public String getSectionKey() {
		return "SECTIONONE";
	}

	public String getName() {
		return "Section Name One";
	}

	public List<Parameter> getParameters() {
		List<Parameter> pList = new ArrayList<>();
		
		List<ETLStepMetadataFieldBean> defaultFields = step.getDefaultMetadataFields();

		ParameterImpl p = new ParameterImpl();

		p = new ParameterImpl();
		p.setProperty("TEXTFIELD");
		p.setInputType(InputType.SELECT);
		p.addViewOption("combineValues", true);
		p.setName("Text field");
		for(ETLStepMetadataFieldBean field: defaultFields) {
			String fieldname=field.getFieldName();
			if (!fieldname.equals("Sentiment Magnitude")&&!fieldname.equals("Sentiment Score")&&field.getFieldType().equals("TEXT")){
				p.addPossibleValue(field.getEtlStepMetadataFieldUUID(),fieldname);
			}
		}
		pList.add(p);
		p = new ParameterImpl();
		p.setInputType(InputType.TEXTBOX);
		p.setProperty("APIKEY");
		p.setName("API Key");
		pList.add(p);

		return pList;
	}

	@Override
	public List<ParameterDisplayRule> getDisplayRules() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public GeneralPanelOptions getSectionOptions() {
		return null;
	}

	@Override
	public Map<String, ?> getData() {
		AbstractETLStep step = (AbstractETLStep)this.step;
		for(Parameter p: this.getParameters()) {
			data.put(p.getProperty(), convertParameterToJSON(step.getStepOption(p.getProperty())));
		}
		return data;
	}

}
