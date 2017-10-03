package pmml;

import com.hof.mi.data.ReportViewSourceBean;
import com.hof.mi.etl.models.FieldObject;
import com.hof.mi.etl.step.ETLDataType;
import com.hof.mi.models.report.CustomValue;
import com.hof.mi.process.MIReportProcess;
import com.hof.mi.util.ViewCache;
import com.hof.parameters.ParameterValueLoader;
import com.hof.pool.DBUtil;
import com.hof.util.ActionErrorsException;
import com.hof.util.DBAction;
import com.hof.util.UtilInteger;
import com.hof.util.UtilString;
import com.hof.util.YFLogger;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.bind.JAXBException;

import org.dmg.pmml.FieldName;
import org.dmg.pmml.PMML;
import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.InputField;
import org.jpmml.evaluator.ModelEvaluatorFactory;
import org.jpmml.evaluator.OutputField;
import org.jpmml.evaluator.TargetField;
import org.xml.sax.SAXException;

public class PMMLLoader implements ParameterValueLoader
{
	@Override
	public Map<String, List<CustomValue<?>>> getUpdatedPossibleValues(Map<String, Object> arg0,
			Map<String, Object> arg1) throws Exception {
		System.out.println("###################");
		System.out.println("###################");
		System.out.println("###################");
		System.out.println("###################");
		System.out.println("###################");

		Set<String> mykeys = arg0.keySet();
		for (String key: mykeys){
			System.out.println(key);
		}
		Map<String, List<CustomValue<?>>> results =  new HashMap();
		return results;
	}
	public Evaluator getEvaluator(String filepath){
		PMML pmml = null;
		if (filepath!=null){
			InputStream is;
			try {
				is = new FileInputStream(filepath);
				pmml = org.jpmml.model.PMMLUtil.unmarshal(is);
			} catch (SAXException | JAXBException | FileNotFoundException e) {
				return null;
			}
			ModelEvaluatorFactory modelEvaluatorFactory = ModelEvaluatorFactory.newInstance();
			Evaluator evaluator = modelEvaluatorFactory.newModelEvaluator(pmml);
			evaluator.verify();
			return evaluator;
		}else{
			return null;
		}

	}
	public List<String> getPMMLFields(Evaluator evaluator){
		System.out.println("in hur1");

		List<String> pmmlFields = new ArrayList<>();
		List<InputField> inputFields = evaluator.getInputFields();
		for(InputField inputField : inputFields){
			pmmlFields.add(inputField.getName().toString());
		}
		return pmmlFields;
	}	

	public List<CustomValue<?>> getPMMLOutput(Evaluator evaluator){
		LinkedList pmmlOutputs = new LinkedList();
		if (evaluator==null) return pmmlOutputs;
		List<OutputField> outputFields = evaluator.getOutputFields();
		for(OutputField outputField : outputFields){
			pmmlOutputs.add(outputField.getName().toString());
		}
		List<TargetField> targetFields = evaluator.getTargetFields();
		for(TargetField targetField : targetFields){
		   FieldName tf=targetField.getName();
		    	String outputname = "Default Target";
		    	if (tf!=null){
		    		outputname = tf.toString();
		    	}
		    	pmmlOutputs.add(new CustomValue(outputname,outputname));
		}
		return pmmlOutputs;
	}

	
}