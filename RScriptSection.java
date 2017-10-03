package rstep;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
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
import com.hof.parameters.ParameterValidation;


public class RScriptSection extends ParameterSection {

	private ETLStep step;
	Map<String, Object> data;
	public RScriptSection(ETLStep step) {
		this.step = step;
		data = new HashMap<>();

	}
	
	@Override
	public String getSectionKey() {
		return "RSECTION";
	}

	public String getName() {
		return "R Section";
	}

	public List<Parameter> getParameters() {
		List<Parameter> pList = new ArrayList<>();
		
		ParameterImpl external = new ParameterImpl();

	    external.setName("External connection");
	    external.setProperty("external");
	    external.setInputType(InputType.TOGGLE);
	    external.setDefaultValue(Boolean.valueOf(false));
	    pList.add(external);
	    
	    ParameterImpl host = new ParameterImpl();
	    host.setName("Host");
	    host.setProperty("host");
	    host.setInputType(InputType.TEXTBOX);
	    host.addViewOption("width", "200px");
	    host.addDisplayRule(new ParameterDisplayRule("AND", "external", Boolean.valueOf(true), false));
	    pList.add(host); 
	    
	    ParameterImpl port = new ParameterImpl();
	    port.setName("Port");
	    port.setProperty("port");
	    port.setInputType(InputType.TEXTBOX);
	    port.addViewOption("width", "50px");
	    ParameterValidation validation = new ParameterValidation();
	    validation.setNumeric(true);
	    port.addDisplayRule(new ParameterDisplayRule("AND", "external", Boolean.valueOf(true), false));
	    port.setValidationRules(validation);
	    port.setDefaultValue(6311);
	    pList.add(port); 
	    

	    
	    ParameterImpl f = new ParameterImpl();
	    f.setName("R Script filepath");
	    f.setProperty("filepath");
	    f.setInputType(InputType.TEXTBOX);
	    f.setParameterClassName("rFilepath");
	    f.addViewOption("width", "350px");
	    validation = new ParameterValidation();
	    validation.setNotEmpty(true);
	    f.setValidationRules(validation);
	    pList.add(f); 
	    
	    ParameterImpl ret = new ParameterImpl();
	    ret.setName("Return Method");
	    ret.setProperty("return");
	    ret.setInputType(InputType.RADIO);
	    ret.setParameterClassName("rFilepath");
	    ret.addPossibleValue("append", "Append");
	    ret.addPossibleValue("overwrite", "Overwrite");

	    ret.addViewOption("width", "350px");
	    validation = new ParameterValidation();
	    validation.setNotEmpty(true);
	    ret.setValidationRules(validation);
	    pList.add(ret); 
	    
	    
	    ParameterImpl k = new ParameterImpl();
	    k.setName("Number of Added Fields");
	    k.setProperty("numExtraFields");
	    k.setInputType(InputType.TEXTBOX);
	    k.setParameterClassName("extraFields");
	    k.addViewOption("width", "50px");
	    k.addDisplayRule(new ParameterDisplayRule("AND", "return", "append", false));

	    validation = new ParameterValidation();
	    k.setValidationRules(validation);
	    pList.add(k); 
	    
	    
	    ParameterImpl t = new ParameterImpl();
	    t.setName("Totoal Number of Fields");
	    t.setProperty("numTotalFields");
	    t.setInputType(InputType.TEXTBOX);
	    t.setParameterClassName("totalFields");
	    t.addViewOption("width", "50px");
	    t.addDisplayRule(new ParameterDisplayRule("AND", "return", "overwrite", false));

	    validation = new ParameterValidation();
	    t.setValidationRules(validation);
	    pList.add(t); 
	    
	    

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
		// TODO Auto-generated method stub
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
