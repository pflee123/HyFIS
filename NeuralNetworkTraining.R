GradientCalculation <- function(object){
  neurons <- object$neurons
  weights <- object$weights
  bias <- object$bias
  y.result <- neurons[[length(neurons)]]
  y <- object$data[,ncol(object$data)]
  error <- object$err.fct(y.result, y)
  
  output.gradient <- attr(error,"gradient")[,1]
  
  temp.gradient <- output.gradient
  
  weights.gradient <- list()
  neurons.gradient <- list()
  
  output.layer.indx <- length(object$layer.num)
  output.act.deriv <- attr(neurons[[output.layer.indx]],"gradient")
  error.deriv <- attr(object$metric,"gradient")[,"x"]
  if(is.null(output.act.deriv))
    output.act.deriv <- 1
  neurons.gradient[[output.layer.indx]] <- output.act.deriv * error.deriv
  
  for(layer.indx in length(weights):1){

    # to neuron gradient
    temp.gradient <- as.matrix(neurons.gradient[[layer.indx+1]], ncol = object$layer.num[layer.indx + 1])
    bias.matrix <- matrix(1, nrow = nrow(object$data), ncol = bias[layer.indx])
    # output
    output <- neurons[[layer.indx]]

    weights.gradient[[layer.indx]] <- rbind(crossprod(output, temp.gradient), t(crossprod(temp.gradient, bias.matrix)))
    
    if(layer.indx != 1){
      act.deriv <- matrix(attr(neurons[[layer.indx]],"gradient"), nrow = nrow(object$data))
      
      weights.temp <- weights[[layer.indx]]
      weights.temp <- weights.temp[1:ncol(act.deriv),]
      
      neurons.gradient[[layer.indx]] <- temp.gradient %*% weights.temp * act.deriv
    }
  }
  
  object$neurons_gradient <- neurons.gradient
  object$weights_gradient <- weights.gradient
  
  object
}

Backpropagation <- function(object){
  
  if(is.null(object$weights_gradient))
    object <- GradientCalculation(object)
  
  if(is.null(object$learningrate))
    object$learningrate <- 1
  for(indx in 1:length(object$weights)){
    object$weights[[indx]] <- object$weights[[indx]] - object$weights_gradient[[indx]] * object$learningrate
  }
  
  object$old_gradients <- object$weights_gradient
  object
}