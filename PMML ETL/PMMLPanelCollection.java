package pmml;


import java.util.ArrayList;
import java.util.List;

import com.hof.mi.etl.step.ETLStep;
import com.hof.parameters.ParameterDisplayRule;
import com.hof.parameters.ParameterPanel;
import com.hof.parameters.ParameterPanelCollection;

public class PMMLPanelCollection extends ParameterPanelCollection {

	private ETLStep step;
	public PMMLPanelCollection(ETLStep step) {
		this.step = step;
	}
	
	public String getName() {
		return "panelName";
	}

	public String getDescription() {
		return "panelDescription";
	}

	public List<ParameterPanel> getPanels() {
		List<ParameterPanel> ppList = new ArrayList<>();
		ppList.add(new SetupPanel(step));
		return ppList;
	}

	public List<ParameterDisplayRule> getDisplayRules() {
		return null;
	}

}
