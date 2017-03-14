Prediction.NeuralNetwork <- function(object, data){
  neurons <- object$neurons
  
  neurons[[1]] <- as.matrix(data)
  
  weights <- object$weights
  
  act.fct <- object$act.fct
  
  for(indx in 1:length(weights)){
    weight <- weights[[indx]]
    neuron <- neurons[[indx]]
    neurons[[indx + 1]] <- neuron %*% weight
    if(indx != length(weights)){
      act.fct.indx <- act.fct[[indx]]
      neurons[[indx + 1]] <- act.fct.indx(neurons[[indx + 1]])
    }
  }
  
  result <- neurons[[length(neurons)]]
  object$neurons <- neurons
  list(result = result, object = object)
}