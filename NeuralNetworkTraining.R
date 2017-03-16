GradientCalculation <- function(object){
  neurons <- object$neurons
  weights <- object$weights
  act.deriv <- object$act.deriv 
  bias <- object$bias
  
  weights.gradient <- list()
  
  layer.num <- object$layer.num
  output.layer.indx <- length(object$layer.num)
  output.act.deriv <- act.deriv[[output.layer.indx]]
  error.deriv <- attr(object$metric,"gradient")[,"x"]
  if(is.null(output.act.deriv))
    output.act.deriv <- 1
  neurons.gradient <- output.act.deriv * error.deriv
  neurons.gradient <- as.matrix(neurons.gradient, ncol = layer.num[layer.indx + 1])
  
  for(layer.indx in length(weights):1){

    # to neuron gradient
    bias.matrix <- matrix(1, nrow = nrow(object$data), ncol = bias[layer.indx])
    # output
    output <- cbind(neurons[[layer.indx]], bias.matrix)

    weights.gradient[[layer.indx]] <- crossprod(output, neurons.gradient)
    
    rownames(weights.gradient[[layer.indx]]) <- c(paste("FROM", 1:layer.num[layer.indx], sep = '_'),paste("BIAS", 1:bias[layer.indx], sep = '_'))
    colnames(weights.gradient[[layer.indx]]) <- paste("TO", 1:layer.num[layer.indx+1], sep = '_')
    
    if(layer.indx != 1){
      act.deriv.temp <- matrix(act.deriv[[layer.indx]], ncol = layer.num[layer.indx])
      
      weights.temp <- weights[[layer.indx]]
      weights.temp <- weights.temp[1:layer.num[layer.indx],]
      
      neurons.gradient <- act.deriv.temp * tcrossprod(temp.gradient, weights.temp)
    }
  }
  
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