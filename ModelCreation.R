Train <- function(data, model.type, ...){
  switch(model.type,
         WM = Train.WM(data,model.type, ...))
}

Train.WM <- function(data, model.type = 'WM', range.data, num.labels, control = NULL){
  data.norm <- norm.data(data, range.data, min.scale = 0, max.scale = 1)
  if(length(num.labels) != ncol(data)){
    num.labels <- rep_len(num.labels, ncol(data))
  }
  
  default.control <- list(num.labels = 10, type.mf = "GAUSSIAN", type.tnorm = "PRODUCT", type.snorm = "SUM", type.defuzz = "COG")
  control <- MissingDataFilled(control, default.control)
  
  type.mf <- control$type.mf
  type.tnorm <- control$type.tnorm
  type.snorm <- control$type.snorm
  type.defuzz <- control$type.defuzz
  
  object <- WM(data.norm, num.labels, range.data)
  
  fitted.norm <- predict.norm(object, data.norm[,-ncol(data.norm)])
  fitted <- denorm.data(fitted.norm$predicted.val, range.data[,ncol(range.data)], min.scale = 0, max.scale = 1)
  
  object$fitted <- fitted
  object$x <- data[,-ncol(data)]
  object$y <- data[, ncol(data)]
  
  return(object)
}

MissingDataFilled <- function(control, default.control){
  if(is.null(control))
    control <- default.control
  else{
    for(iter in names(default.control)){
      if(is.null(control[[iter]]))
        control[[iter]] <- default.control[[iter]]
    }
  }
  
  return(control)
}