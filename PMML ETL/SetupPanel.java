package pmml;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.hof.mi.etl.example.ExampleETLSection;
import com.hof.mi.etl.example.ExampleETLSection2;
import com.hof.mi.etl.example.ExampleETLSectionCheckBoxFields;
import com.hof.mi.etl.example.ExampleETLSectionFileUpload;
import com.hof.mi.etl.step.ETLStep;
import com.hof.parameters.GeneralPanelOptions;
import com.hof.parameters.ParameterDisplayRule;
import com.hof.parameters.ParameterPanel;
import com.hof.parameters.ParameterSection;
import com.hof.util.UtilString;

public class SetupPanel extends ParameterPanel {

	private ETLStep step = null;
	public SetupPanel(ETLStep step) {
		this.step = step;
	}
	
	@Override
	public String getPanelKey() {
		return "SETUPPANEL";
	}
	@Override
	public String getName() {
		return "SETUPPANEL";
	}
	public String getParameterPanelClassName(){
		return "ETLConfigPanel";
	}
	public String getDescription() {
		return "asdf";
	}

	@Override
	public List<ParameterSection> getSections() {
		System.out.println("getting sections");
		List<ParameterSection> psList = new ArrayList<>();
		psList.add(new SetupSection(step));
		//psList.add(new MatchSection(step));

		return psList;
	}

	@Override
	public List<ParameterDisplayRule> getDisplayRules() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public GeneralPanelOptions getGeneralOptions() {
		GeneralPanelOptions gpo = new GeneralPanelOptions();
		gpo.setSaveButton(true);
		HashMap localHashMap2 = new HashMap();
	    localHashMap2.put("flat", Boolean.valueOf(true));
	    localHashMap2.put("cssClass", "configure-apply-btn");
	    ((GeneralPanelOptions)gpo).setSaveButtonOptions(localHashMap2);
		gpo.setSaveText("Load");
		gpo.setExpandable(true);
		return gpo;
	}

}
