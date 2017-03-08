ModelCreation <- function(Dataset.train, range.data = NULL, model.type = 'WM', control = NULL, inputId, conn, Input_Discription = NULL){
  
  require(jsonlite)
  source('~/FusionProject/R Code/FUSION_HyFIS/InsertDatabase.R')
  source('~/FusionProject/R Code/FUSION_HyFIS/ModelTraining.R')
  
  jsonConfiguration <- serializeJSON(control)
  
  Model_Discription <- model.type #readline(prompt = paste0("Enter ",model.type," model description:"))
  model <- list(Type = model.type, Configuration = jsonConfiguration, Model_Discription = Model_Discription)
  
  object <- ModelTraining(Dataset.train, range.data, model.type, control)
  
  jsonObject <- serializeJSON(object)
  
  Experiment_Reason <- sprintf("Using %s and %s to predict", model.type, Input_Discription)#readline(prompt = "Enter this experiment season:")
  experiment <- list(Training_Parameters = jsonObject, Experiment_Reason = Experiment_Reason)
  
  return(list(object = object, experiment = experiment, model = model, InputId = inputId))
}