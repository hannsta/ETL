package pmml;


import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.xml.bind.JAXBException;

import org.dmg.pmml.Entity;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.PMML;
import org.jpmml.evaluator.Computable;
import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.FieldValue;
import org.jpmml.evaluator.HasEntityId;
import org.jpmml.evaluator.HasEntityRegistry;
import org.jpmml.evaluator.HasProbability;
import org.jpmml.evaluator.InputField;
import org.jpmml.evaluator.ModelEvaluatorFactory;
import org.jpmml.evaluator.OutputField;
import org.jpmml.evaluator.TargetField;
import org.xml.sax.SAXException;

import com.google.common.collect.BiMap;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.hof.data.EntityBean;
import com.hof.mi.data.Wire;
import com.hof.mi.etl.ETLException;
import com.hof.mi.etl.ETLStepCategory;
import com.hof.mi.etl.data.ETLStepMetadataFieldBean;
import com.hof.mi.etl.process.StepBuilder;
import com.hof.mi.etl.step.AbstractETLRowStep;
import com.hof.mi.etl.step.AbstractETLStep;
import com.hof.mi.etl.step.ETLStep;
import com.hof.parameters.ParameterPanelCollection;


public class PMMLStep extends AbstractETLRowStep {
	public int counter = 0;
	public Evaluator evaluator;
	public Map<String, String> outputUUIDs = new HashMap();
	@Override
	public String getDefaultName() {
		return "PMML Model Prediction";
	}

	@Override
	public String getDefaultDescription() {
		return "Run predictions against a model stored as a PMML file.";
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
		ParameterPanelCollection mypan = new PMMLPanelCollection(this);
		return mypan;
	}

	@Override
	public void setupGeneratedFields() {
		String opt = getStepOption("Data_Type");
		List<ETLStepMetadataFieldBean> defaultFields = getDefaultMetadataFields();
		List<String> outputFields = getOutputs();
		if (defaultFields!=null && defaultFields.size()>0){
			this.outputUUIDs=new HashMap();
			for (String outputField : outputFields){
				ETLStepMetadataFieldBean newField = defaultFields.get(0).duplicate();
				String tempUUID=UUID.randomUUID().toString();
				newField.setEtlStepMetadataFieldUUID(tempUUID);
				System.out.println(newField.getFieldType());
				newField.setFieldName(outputField);
				if (opt!=null){
					newField.setFieldType(opt);
				}
				newField.setEBChangeStat(EntityBean.EB_INSERT);
				addDefaultMetadataField(newField);
				this.outputUUIDs.put(outputField, tempUUID);
			}
		}
		if (getStepOption("uploadFile")!=null && this.evaluator!=null){
			setEvaluator();
		}
	}

	@Override
	public Map<String, String> getDefaultInternalOptions() {
		return null;
	}

	public void setEvaluator(){
		PMML pmml = null;
		String filepath = getStepOption("file_name");
		InputStream is;
		try {
			is = new FileInputStream(filepath);
			pmml = org.jpmml.model.PMMLUtil.unmarshal(is);
		} catch (SAXException | JAXBException | FileNotFoundException e) {
			this.evaluator=null;
		}
		ModelEvaluatorFactory modelEvaluatorFactory = ModelEvaluatorFactory.newInstance();
		Evaluator evaluator = modelEvaluatorFactory.newModelEvaluator(pmml);
		evaluator.verify();
		this.evaluator=evaluator;
	}
	@Override
	public Map<String, String> getValidatedStepOptions() {
		return null;
	}

	@Override
	protected boolean processWireData(List<ETLStepMetadataFieldBean> fields) throws ETLException, InterruptedException {
		setEvaluator();

		List<InputField> inputFields = this.evaluator.getInputFields();
		Map<String, String> nameAndId = new HashMap();
		
		for (ETLStepMetadataFieldBean field :fields){
			nameAndId.put(field.getFieldName(), field.getEtlStepMetadataFieldUUID());
		}	
		Map<FieldName, FieldValue> arguments = new LinkedHashMap<>();
		for (InputField PMMLField : inputFields){
			String tempUUID = nameAndId.get(getStepOption("FieldMatch"+PMMLField.getName().toString()));
			Wire<Object,String> resultWire = getWireForField(tempUUID);
			arguments.put(PMMLField.getName(), PMMLField.prepare(resultWire.getValue()));
		}
		Map<FieldName, ?> results = this.evaluator.evaluate(arguments);
			//Get the "Target Field" result
		List<TargetField> targetFields = this.evaluator.getTargetFields();
		for(TargetField targetField : targetFields){
			
			FieldName targetFieldName = targetField.getName();
			Object targetFieldValue = results.get(targetFieldName);
			if(targetFieldValue instanceof Computable){
			    Computable computable = (Computable)targetFieldValue;
			    Object unboxedTargetFieldValue = computable.getResult();
				String outputname = "Default Target";
				if (targetFieldName!=null){
				    	outputname = targetFieldName.toString();
				}
			    String fieldUUID = this.outputUUIDs.get(targetFieldName.toString());
				if (fieldUUID!=null){
			    	Wire<Object, String> wire = this.getWireForField(fieldUUID);
			    	wire.send(unboxedTargetFieldValue);			    	
			    }
			
			    if(targetFieldValue instanceof HasEntityId){
			    	HasEntityId hasEntityId = (HasEntityId)targetFieldValue;
			    	HasEntityRegistry<?> hasEntityRegistry = (HasEntityRegistry<?>)this.evaluator;
			    	BiMap<String, ? extends Entity> entities = hasEntityRegistry.getEntityRegistry();
			    	Entity winner = entities.get(hasEntityId.getEntityId());
			    	if(targetFieldValue instanceof HasProbability){
			    		HasProbability hasProbability = (HasProbability)targetFieldValue;
			    		Double winnerProbability = hasProbability.getProbability(winner.getId());
			    	}
			    }
			}
			List<OutputField> outputFields = this.evaluator.getOutputFields();
			for(OutputField outputField : outputFields){
				FieldName outputFieldName = outputField.getName();
			    Object outputFieldValue = results.get(outputFieldName);
			    String fieldUUID = this.outputUUIDs.get(outputFieldName.toString());
				if (fieldUUID!=null){
			    	Wire<Object, String> wire = this.getWireForField(fieldUUID);
			    	wire.send(outputFieldValue);			    	
			    }
			}

		}
		return true;
	}


	private List<String> getOutputs() {
		Type t = new TypeToken<ArrayList<String>>(){}.getType();
		Gson g = new Gson();
		String opt = getStepOption("PMMLOutput");
		if(opt == null) return new ArrayList<>();
		List<String> PMMLOutputs = g.fromJson(opt, t);
		return PMMLOutputs;
	}

	public void parseData(Map<String, Object> data) {
	}

}
