Prediction.NeuralNetwork <- function(object, data){
  neurons <- object$neurons
  
  bias <- object$bias
  
  neurons[[1]] <- as.matrix(data)
  
  weights <- object$weights
  
  act.fct <- object$act.fct
  act.deriv <- list()
  
  for(indx in 1:length(weights)){
    weight <- weights[[indx]]
    bias.matrix <- matrix(1, nrow = nrow(neurons[[indx]]), ncol = bias[indx])
    neuron <- cbind(neurons[[indx]], bias.matrix)
    neurons[[indx + 1]] <- neuron %*% weight
    act.fct.indx <- act.fct[[indx+1]]
    neurons[[indx + 1]] <- act.fct.indx(neurons[[indx + 1]])
    act.deriv[[indx + 1]] <- matrix(attr(neurons[[indx + 1]],"gradient"), nrow = nrow(neurons[[indx]]))
  }
  
  result <- neurons[[length(neurons)]]
  object$neurons <- neurons
  object$act.deriv <- act.deriv
  object$teaching_input <- result
  object$error_vector <- result - object$output_vector
  object$metric <- object$err.fct(result, object$output_vector)
  
  list(result = result, object = object)
}