package sentimentAnalysis;


import java.util.ArrayList;
import java.util.List;

import com.hof.mi.etl.step.ETLStep;
import com.hof.parameters.GeneralPanelOptions;
import com.hof.parameters.ParameterDisplayRule;
import com.hof.parameters.ParameterPanel;
import com.hof.parameters.ParameterSection;
import com.hof.util.UtilString;

public class GoogleSentimentPanel extends ParameterPanel {

	private ETLStep step = null;
	public GoogleSentimentPanel(ETLStep step) {
		this.step = step;
	}
	
	@Override
	public String getPanelKey() {
		return "PANELONE";
	}

	@Override
	public String getName() {
		return UtilString.getResourceString("mi.text.panel");
	}

	public String getDescription() {
		return null;
	}

	@Override
	public List<ParameterSection> getSections() {
		List<ParameterSection> psList = new ArrayList<>();
		
		psList.add(new GoogleSentimentSection(step));

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
		gpo.setSaveText(UtilString.getResourceString("mi.text.apply"));
		return gpo;
	}

}
