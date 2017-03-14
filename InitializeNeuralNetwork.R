InitializeFunction <- function (act.fct, err.fct, layer.num) {
    
  act.fct.layer <- list()
  indx <- 1
  for(act.fct.indx in act.fct){
    if(act.fct.indx == "tanh"){
      act.fct.indx <- function(x){
        tanh(x)
      }
    }
    if(act.fct.indx == "logistic"){
      act.fct.indx <- function(x){
        1/(1 + exp(-x))
      }
    }
    if (is.function(act.fct.indx)) {
      act.fct.indx <- differentiate(act.fct.indx)
      attr(act.fct.indx, "type") <- "activation"
    }
    
    act.fct.layer[[indx]] <- act.fct.indx
    indx <- indx + 1
  }
  if(err.fct == "sse"){
    err.fct <- function(x, y){
      1/2 * (y - x)^2
    }
  }
  if (is.function(err.fct)) {
    err.fct <- differentiate(err.fct)
    attr(err.fct, "type") <- "metric"
  }
  
  act.fct <- rep_len(act.fct.layer, length.out = length(layer.num)-2)
  return(list(err.fct = err.fct, act.fct = act.fct))
}

#' @param exclude triple element{layer.num, row.num(from), col.num(to)}
StartWeightsGenernalization <- function (model.list, hidden, startweights, range.initial, exclude, constant.weights) {
  
  input.count <- length(model.list$variables)
  output.count <- length(model.list$response)
  
  hidden.count <- hidden[hidden != 0]
  
  layer.num <- c(input.count, hidden.count, output.count)
  if(is.null(startweights))
    weights <- list()
  else
    weights <- startweights
  for(indx in 1:(length(layer.num)-1)){
    if(indx > length(weights)|| is.null(weights[[indx]])){
      from <- layer.num[indx]
      to <- layer.num[indx + 1]
      weights.vector <- runif(from * to, -range.initial, range.initial)
      weights.indx <- matrix(weights.vector, nrow = from, ncol = to)
      weights[[indx]] <- weights.indx
    }
  }
  
  if(!is.null(exclude)){
    apply(exclude, 2, function(x){
      weights[[x[1]]][x[2],x[3]] <<- NA
    })
  }
  
  return(weights)
}

NeuralNetwork <- function(data, call, layer.num, weights, model.list, err.fct, act.fct, exclude){
  neurons <- list()
  for(indx in 1:length(layer.num)){
    neurons[[indx]] <- matrix(0, ncol = layer.num[indx])
  }
  
  structure(list(data = data,
                 call = call,
                 layer.num = layer.num,
                 weights = weights,
                 neurons = neurons,
                 model.list = model.list,
                 err.fct = err.fct,
                 act.fct = act.fct,
                 exclude = exclude), class = "NeuralNetwork")
}
