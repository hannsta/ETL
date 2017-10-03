package sentimentAnalysis;


import java.util.ArrayList;
import java.util.List;

import com.hof.mi.etl.step.ETLStep;
import com.hof.parameters.ParameterDisplayRule;
import com.hof.parameters.ParameterPanel;
import com.hof.parameters.ParameterPanelCollection;

public class GoogleSentimentPanelCollection extends ParameterPanelCollection {

	private ETLStep step;
	public GoogleSentimentPanelCollection(ETLStep step) {
		this.step = step;
	}
	
	public String getName() {
		return null;
	}

	public String getDescription() {
		return null;
	}

	public List<ParameterPanel> getPanels() {
		List<ParameterPanel> ppList = new ArrayList<>();
		ppList.add(new GoogleSentimentPanel(step));

		return ppList;
	}

	public List<ParameterDisplayRule> getDisplayRules() {
		return null;
	}

}
