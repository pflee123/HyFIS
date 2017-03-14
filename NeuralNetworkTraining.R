GradientCalculation <- function(object){
  neurons <- object$neurons
  weights <- object$weights
  y.result <- neurons[[length(neurons)]]
  y <- object$data[,ncol(object$data)]
  error <- object$err.fct(y.result, y)
  
  output.gradient <- attr(error,"gradient")[,1]
  
  temp.gradient <- output.gradient
  
  gradient <- list()
  for(layer.indx in length(object$neurons):2){
    gradient[[layer.indx]] <- temp.gradient
    
    act.deriv <- attr(neurons[[layer.indx-1]],"gradient")
    
    output <- neurons[[layer.indx-1]]
    
    if(!is.null(act.deriv)){
      temp.gradient <- temp.gradient * act.deriv
    }
    
    temp.gradient <- temp.gradient * output
  }
  
  object$gradient <- gradient
  
  object
}