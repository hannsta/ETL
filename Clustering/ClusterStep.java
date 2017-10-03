package apacheClustering;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
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
import java.io.PrintStream;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.ml.clustering.Cluster;
import org.apache.commons.math3.ml.clustering.Clusterer;
import org.apache.commons.math3.ml.clustering.DBSCANClusterer;
import org.apache.commons.math3.ml.clustering.DoublePoint;
import org.apache.commons.math3.ml.clustering.FuzzyKMeansClusterer;
import org.apache.commons.math3.ml.clustering.KMeansPlusPlusClusterer;
import org.apache.commons.math3.ml.clustering.MultiKMeansPlusPlusClusterer;
import org.apache.commons.math3.ml.distance.CanberraDistance;
import org.apache.commons.math3.ml.distance.ChebyshevDistance;
import org.apache.commons.math3.ml.distance.DistanceMeasure;
import org.apache.commons.math3.ml.distance.EarthMoversDistance;
import org.apache.commons.math3.ml.distance.EuclideanDistance;
import org.apache.commons.math3.ml.distance.ManhattanDistance;

public class ClusterStep
  extends AbstractETLCachedStep
{
  Map<String, Object> data;
  public String getDefaultName()
  {
    return "Clustering";
  }
  
  public String getDefaultDescription()
  {
    return "Cluster stuff";
  }
  
  public void setupGeneratedFields()
  {
    List<ETLStepMetadataFieldBean> fields = getDefaultMetadataFields();
    if ((fields.size() > 0) && (getStepOption("prediction") == null))
    {
      ETLStepMetadataFieldBean newField = ((ETLStepMetadataFieldBean)fields.get(0)).duplicate();
      newField.setFieldName("Prediction");
      newField.setFieldType("NUMERIC");
      addNewGeneratedField(newField, "prediction");
    }
    if ((Boolean.valueOf(getStepOption("include")).booleanValue()) && (getStepOption("cluster0") == null)) {
      for (int i = 0; i < Integer.valueOf(getStepOption("k")).intValue(); i++)
      {
        ETLStepMetadataFieldBean newField = ((ETLStepMetadataFieldBean)fields.get(0)).duplicate();
        newField.setFieldName("cluster" + Integer.toString(i));
        newField.setFieldType("NUMERIC");
        addNewGeneratedField(newField, "cluster" + Integer.toString(i));
      }
    }
    if (!Boolean.valueOf(getStepOption("include")).booleanValue())
    {
      int i = 0;
      while (getStepOption("cluster" + Integer.toString(i)) != null)
      {
        removeDefaultMetadataField(getStepOption("cluster" + Integer.toString(i)));
        i++;
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
    ETLDataCache dataCache = null;
    List<String> fields = new ArrayList();
    for (String flowUUID : getInputFlowUuids())
    {
      dataCache = getDataCache(flowUUID);
      for (ETLStepMetadataFieldBean field: dataCache.getMetadataFields()){
    	  fields.add(field.getEtlStepMetadataFieldUUID());
      }
    }
    Iterator<Object[]> dataIterator = dataCache.iterator();
    
     List<String> featureList = getFeatureList();
    if (featureList.size() < 2) {
      throw new ETLException(ETLElement.STEP, getUuid(), null, "Need at least 2 features for clustering", null);
    }
    List<DoublePoint> clusterInput = new ArrayList();
    List<Object[]> inputData = new ArrayList();
    while (dataIterator.hasNext())
    {
      Object[] row = (Object[])dataIterator.next();
      double[] rowAsDoubleArray = new double[featureList.size()];
      int featureCount = 0;
      for (int i = 0; i < row.length; i++) {
        if (featureList.contains(fields.get(i)))
        {
          if (row[i] != null) {
            rowAsDoubleArray[featureCount] = new Double(row[i].toString()).doubleValue();
          }
          featureCount++;
        }
      }
      clusterInput.add(new DoublePoint(rowAsDoubleArray));
      inputData.add(row);
    }
    DistanceMeasure distance = getDistanceMeasure(getStepOption("distance"));
    Clusterer clusterer = getClusterer(distance);
    
    List<Cluster<DoublePoint>> clusters = clusterer.cluster(clusterInput);
    HashMap<DoublePoint, Integer> assignments = getAssignments(clusters);
    
    RealMatrix fuzzyMembership = null;
    if ((Boolean.valueOf(getStepOption("include")).booleanValue()) && (getStepOption("algorithm").equals("fuzzy"))) {
      fuzzyMembership = ((FuzzyKMeansClusterer)clusterer).getMembershipMatrix();
    }
    for (int i = 0; i < (inputData).size(); i++)
    {
      DoublePoint inputRow = (DoublePoint)clusterInput.get(i);
      Integer assignment = null;
      if (assignments.get(inputRow) != null) {
        assignment = assignments.get(inputRow);
      }
      String str2 = getFirstOutputFlow();
      ETLStepResult stepResult = getFreshDataPacket(str2);
      
      List<String> assignmentuuid = new ArrayList();
      assignmentuuid.add(getStepOption("prediction"));
      beginInternalTransmission(inputData.get(i), fields);
      beginInternalTransmission(new Object[] { assignment }, assignmentuuid);
      if (fuzzyMembership != null)
      {
        int k = Integer.valueOf(getStepOption("k")).intValue();
        List<String> fuzzuuids = new ArrayList();
        for (int j = 0; j < k; j++) {
          fuzzuuids.add(getStepOption("cluster" + Integer.toString(j)));
        }
        double[] fuzzyRow = fuzzyMembership.getRow(i);
        Object[] outRow = new Object[fuzzyRow.length];
        for (int d = 0; d < fuzzyRow.length; d++) {
          outRow[d] = Double.valueOf(fuzzyRow[d]);
        }
        beginInternalTransmission(outRow, fuzzuuids);
      }
      endInternalTransmission(stepResult);
      emitData(stepResult);
    }
  }
  
  private void displayDataDist(List<DoublePoint> clusterInput, DistanceMeasure distance)
  {
    double min = 0.0D;
    double sum = 0.0D;
    int count = 0;
    while ((count < 2000) || (count < clusterInput.size()))
    {
      Random rand = new Random();
      double dist = distance.compute(((DoublePoint)clusterInput.get(rand.nextInt(clusterInput.size()))).getPoint(), ((DoublePoint)clusterInput.get(rand.nextInt(clusterInput.size()))).getPoint());
      sum += dist;
      if ((dist < min) || (min == 0.0D)) {
        min = dist;
      }
      count++;
    }
    System.out.println("Min: " + Double.toString(min) + "  Avg: " + Double.toString(sum / count));
  }
  
  private Clusterer getClusterer(DistanceMeasure distance)
  {
    String algorithm = getStepOption("algorithm");
    int k = 0;
    int iter = Integer.valueOf(getStepOption("iter")).intValue();
    if (!algorithm.equals("dbscan")) {
      k = Integer.valueOf(getStepOption("k")).intValue();
    }
    if ((algorithm.equals("kmeans")) || (algorithm.equals("multi")))
    {
      KMeansPlusPlusClusterer kmeans = new KMeansPlusPlusClusterer(k, iter, distance);
      int runs = Integer.valueOf(getStepOption("runs")).intValue();
      return new MultiKMeansPlusPlusClusterer(kmeans, runs);
    }
    if (algorithm.equals("fuzzy"))
    {
      double fuzzy = Double.valueOf(getStepOption("fuzzy")).doubleValue();
      return new FuzzyKMeansClusterer(k, fuzzy, iter, distance);
    }
    if (algorithm.equals("dbscan"))
    {
      int minpts = Integer.valueOf(getStepOption("minpts")).intValue();
      double eps = Double.valueOf(getStepOption("eps")).doubleValue();
      return new DBSCANClusterer(eps, minpts, distance);
    }
    return null;
  }
  
  private HashMap<DoublePoint, Integer> getAssignments(List<Cluster<DoublePoint>> clusters)
  {
    HashMap<DoublePoint, Integer> assignments = new HashMap();
    for (int i = 0; i < clusters.size(); i++)
    {
      List<DoublePoint> points = ((Cluster)clusters.get(i)).getPoints();
      for (DoublePoint point : points) {
        assignments.put(point, Integer.valueOf(i + 1));
      }
    }
    return assignments;
  }
  
  private DistanceMeasure getDistanceMeasure(String stepOption)
  {
    if (stepOption.equals("Euclidean")) {
      return new EuclideanDistance();
    }
    if (stepOption.equals("ManhattanDistance")) {
      return new ManhattanDistance();
    }
    if (stepOption.equals("EarthMoversDistance")) {
      return new EarthMoversDistance();
    }
    if (stepOption.equals("ChebyshevDistance")) {
      return new ChebyshevDistance();
    }
    if (stepOption.equals("CanberraDistance")) {
      return new CanberraDistance();
    }
    return null;
  }
  
  private List<String> getFeatureList()
  {
	Type t = new TypeToken<ArrayList<String>>(){}.getType();
    Gson g = new Gson();
    String opt = getStepOption("FEATURE_SELECT");
    if (opt == null) {
      return new ArrayList();
    }
    List<String> regFeatures = g.fromJson(opt, t);
    Map<String, String> currentFieldsMap = getDefaultToInputFieldMap();
    List<String> output = new ArrayList();
    for (String feature : regFeatures) {
      output.add(currentFieldsMap.get(feature));
    }
    return output;
  }
  
  protected ParameterPanelCollection generatePanelCollection()
  {
    ETLStepConfigPanel regressionPanel = new ETLStepConfigPanel("REGRESSION_CONFIG", getETLStepBean().getStepName());
    
    GeneralPanelOptions sectionOptions = new GeneralPanelOptions();
    ETLStepConfigSection clusterSection = new ETLStepConfigSection("Clustering", "");
    clusterSection.setParameterSectionClassName("clusterSection");
    sectionOptions.setShowName(true);
    clusterSection.setSectionOptions(sectionOptions);
    regressionPanel.addSection(clusterSection);
    
    ParameterImpl advanced = new ParameterImpl();
    
    advanced.setName("Advanced configuration");
    advanced.setProperty("advanced");
    advanced.setInputType(InputType.TOGGLE);
    advanced.setDefaultValue(Boolean.valueOf(false));
    clusterSection.addParameter(advanced);
    
    ParameterImpl algorithm = new ParameterImpl();
    algorithm.setName("Clustering Algorithm");
    algorithm.setInputType(InputType.SELECT);
    
    algorithm.setProperty("algorithm");
    algorithm.addPossibleValue("kmeans", "K-means++");
    algorithm.addPossibleValue("fuzzy", "Fuzzy K-means");
    algorithm.addPossibleValue("dbscan", "DBSCAN");
    algorithm.setParameterClassName("clusterStepAlgo");
    algorithm.addViewOption("width", "150px");
    algorithm.addDisplayRule(new ParameterDisplayRule("AND", "advanced", Boolean.valueOf(true), false));
    clusterSection.addParameter(algorithm);
    
    ParameterImpl featureSelect = new ParameterImpl();
    featureSelect.setName("Select Inputs");
    featureSelect.setProperty("FEATURE_SELECT");
    List<String> addedFields = new ArrayList();
    addedFields.add(getStepOption("prediction"));
    if (getStepOption("k") != null) {
      for (int i = 0; i < Integer.valueOf(getStepOption("k")).intValue(); i++) {
        addedFields.add(getStepOption("cluster" + Integer.toString(i)));
      }
    }
    for (ETLStepMetadataFieldBean field : getDefaultMetadataFields()) {
      if ((field.getFieldType().equals("NUMERIC")) && (!addedFields.contains(field.getEtlStepMetadataFieldUUID()))) {
        featureSelect.addPossibleValue(field.getEtlStepMetadataFieldUUID(), field.getFieldName());
      }
    }
    featureSelect.setInputType(InputType.CHECKBOX);
    featureSelect.setParameterClassName("regressionStepFeatureSelect");
    clusterSection.addParameter(featureSelect);
    
    ParameterImpl distanceMeasure = new ParameterImpl();
    distanceMeasure.setName("Distance measurement");
    distanceMeasure.setProperty("distance");
    distanceMeasure.addViewOption("width", "150px");
    distanceMeasure.addPossibleValue("Euclidean", "Euclidean");
    distanceMeasure.addPossibleValue("ManhattanDistance", "Manhattan");
    distanceMeasure.addPossibleValue("EarthMoversDistance", "Earth Movers");
    distanceMeasure.addPossibleValue("ChebyshevDistance", "Chebyshev");
    distanceMeasure.addPossibleValue("CanberraDistance", "Canberra");
    distanceMeasure.addDisplayRule(new ParameterDisplayRule("AND", "advanced", Boolean.valueOf(true), false));
    
    distanceMeasure.setInputType(InputType.SELECT);
    distanceMeasure.setParameterClassName("clusterStepDistance");
    distanceMeasure.setDefaultValue("Euclidean");
    
    clusterSection.addParameter(distanceMeasure);
    
    ParameterImpl k = new ParameterImpl();
    k.setName("Number of clusters");
    k.setProperty("k");
    k.setInputType(InputType.TEXTBOX);
    k.setParameterClassName("clusterStepK");
    k.addViewOption("width", "50px");
    k.addDisplayRule(new ParameterDisplayRule("AND", "algorithm", new Object[] { "dbscan", "dbscan" }, true));
    ParameterValidation validation = new ParameterValidation();
    validation.setNumeric(true);
    validation.setNotEmpty(true);
    
    validation.setNotEmpty(true);
    k.setValidationRules(validation);
    clusterSection.addParameter(k);
    
    ParameterImpl fuzzy = new ParameterImpl();
    fuzzy.setName("Fuzziness");
    fuzzy.setProperty("fuzzy");
    fuzzy.setInputType(InputType.TEXTBOX);
    fuzzy.addViewOption("width", "50px");
    fuzzy.setDefaultValue("2");
    fuzzy.setParameterClassName("clusterStepFuzzy");
    fuzzy.addDisplayRule(new ParameterDisplayRule("AND", "advanced", Boolean.valueOf(true), false));
    fuzzy.addDisplayRule(new ParameterDisplayRule("AND", "algorithm", new Object[] { "fuzzy", "fuzzy" }, false));
    validation = new ParameterValidation();
    validation.setNumeric(true);
    validation.setNotEmpty(true);
    
    fuzzy.setValidationRules(validation);
    clusterSection.addParameter(fuzzy);
    
    ParameterImpl includeMatrix = new ParameterImpl();
    includeMatrix.setName("Include membership matrix");
    includeMatrix.setProperty("include");
    includeMatrix.setInputType(InputType.TOGGLE);
    includeMatrix.setDefaultValue(Boolean.valueOf(false));
    includeMatrix.addDisplayRule(new ParameterDisplayRule("AND", "advanced", Boolean.valueOf(true), false));
    includeMatrix.addDisplayRule(new ParameterDisplayRule("AND", "algorithm", new Object[] { "fuzzy", "fuzzy" }, false));
    clusterSection.addParameter(includeMatrix);
    
    ParameterImpl runs = new ParameterImpl();
    runs.setName("Number of models to run");
    runs.setProperty("runs");
    runs.setInputType(InputType.TEXTBOX);
    runs.addViewOption("width", "50px");
    runs.setParameterClassName("clusterStepRuns");
    runs.setDefaultValue("30");
    runs.addDisplayRule(new ParameterDisplayRule("AND", "advanced", Boolean.valueOf(true), false));
    runs.addDisplayRule(new ParameterDisplayRule("AND", "algorithm", new Object[] { "kmeans", "kmeans" }, false));
    validation = new ParameterValidation();
    validation.setNumeric(true);
    validation.setNotEmpty(true);
    
    runs.setValidationRules(validation);
    clusterSection.addParameter(runs);
    
    ParameterImpl eps = new ParameterImpl();
    eps.setName("Maximum search radius");
    eps.setProperty("eps");
    eps.setInputType(InputType.TEXTBOX);
    eps.addViewOption("width", "50px");
    eps.setParameterClassName("clusterStepeps");
    eps.setDefaultValue("1");
    eps.addDisplayRule(new ParameterDisplayRule("AND", "advanced", Boolean.valueOf(true), false));
    eps.addDisplayRule(new ParameterDisplayRule("AND", "algorithm", new Object[] { "dbscan", "dbscan" }, false));
    validation = new ParameterValidation();
    validation.setNumeric(true);
    validation.setNotEmpty(true);
    eps.setValidationRules(validation);
    clusterSection.addParameter(eps);
    
    ParameterImpl minpts = new ParameterImpl();
    minpts.setName("Minimum points needed for new cluster");
    minpts.setProperty("minpts");
    minpts.setInputType(InputType.TEXTBOX);
    minpts.addViewOption("width", "50px");
    minpts.setParameterClassName("clusterStepPts");
    minpts.setDefaultValue("30");
    minpts.addDisplayRule(new ParameterDisplayRule("AND", "advanced", Boolean.valueOf(true), false));
    minpts.addDisplayRule(new ParameterDisplayRule("AND", "algorithm", new Object[] { "dbscan", "dbscan" }, false));
    validation = new ParameterValidation();
    validation.setNumeric(true);
    validation.setNotEmpty(true);
    minpts.setValidationRules(validation);
    clusterSection.addParameter(minpts);
    
    ParameterImpl iter = new ParameterImpl();
    iter.setName("Maximum iterations");
    iter.setProperty("iter");
    iter.setInputType(InputType.TEXTBOX);
    iter.addViewOption("width", "50px");
    iter.addDisplayRule(new ParameterDisplayRule("AND", "advanced", Boolean.valueOf(true), false));
    iter.setParameterClassName("clusterStepIter");
    iter.setDefaultValue("10000");
    validation = new ParameterValidation();
    validation.setNumeric(true);
    validation.setNotEmpty(true);
    iter.setValidationRules(validation);
    clusterSection.addParameter(iter);
    
    GeneralPanelOptions panelOptions = new GeneralPanelOptions();
    panelOptions.setSaveButton(true);
    panelOptions.setSaveText("Apply");
    
    regressionPanel.setGeneralOptions(panelOptions);
    
    ETLStepPanels regressionPanelCollection = new ETLStepPanels();
    regressionPanelCollection.addPanel(regressionPanel);
    return regressionPanelCollection;
  }
  

}
