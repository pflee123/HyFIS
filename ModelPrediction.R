Prediction <- function(object, newdata){
  UseMethod("Prediction", object)
}

Prediction.RuleBasedModel <- function(object, newdata){
  range.data.ori <- object$range.data.ori
  range.input.ori <- range.data.ori[,-ncol(range.data.ori)]
  range.output.ori <- range.data.ori[,ncol(range.data.ori)]
  data.norm <- norm.data(newdata, range.input.ori, min.scale = 0, max.scale = 1)

  res <- predict.norm(object, data.norm)

  res.denorm <- denorm.data(res$predicted.val, range.output.ori, min.scale = 0, max.scale = 1)

  return(res.denorm)
}

predict.norm <- function(object, newdata, parallel = TRUE){
  ## get all of parameters
  # range.output <- object$range.data.ori[, ncol(object$range.data.ori), drop = FALSE]
  num.input <- object$input_num
  num.output <- object$output_num
  
  # number of labels for each variables
  num.labels <- object$num.labels
  num.labels.input <- num.labels[1:num.input]
  num.labels.output <- num.labels[num.input+1:num.output]
  cum.labels <- cumsum(num.labels)
  
  input_membership <- object$membership_function[,1:cum.labels[num.input]]
  output_membership <- object$membership_function[,-c(1:cum.labels[num.input])]
  ## change linguistic terms/labels to be unique
  # temp <- ch.unique.fuzz(object$type.model, object$rule, object$varinp.mf, object$varout.mf,num.varinput, object$num.labels)
  
  rule <- object$rule
  names.input <- object$mf_label_name[1:cum.labels[num.input]]
  names.output <- object$mf_label_name[-c(1:cum.labels[num.input])]
  names.mf <- object$mf_label_name
  
  type.defuz <- object$type.defuz
  type.tnorm <- object$type.tnorm
  type.snorm <- object$type.snorm
  type.model <- object$type.model

  
  ###################
  ### I. Fuzzification Module
  ### In this function, we convert crisp value into linguistic value based on the data and parameter of membership function.
  ### There are several membership function can be used such as triangular, trapezoid, gaussian and logistic/sigmoid.
  ###################
  ### y2
  
  MF <- fuzzifier(newdata, num.input, num.labels.input, input_membership)
  
  ###################
  ### II. Inference Module
  ### In this function, we will calculate the confidence factor on antecedent for each rule. We use AND, OR, NOT operator. 
  ###################
  ### y3
  
  ncol.MF <- ncol(MF)
  colnames(MF) <- c(names.input)
  
  miu.rule <- inference(MF, rule, names.input, type.tnorm, type.snorm)
    
  miu.rule.indx <- miu.rule$miu.rule.indx
  miu.rule <- miu.rule$miu.rule
  
  ###################
  ### III. Defuzzification Module
  ### In this function, we calculate and convert linguistic value back into crisp value. 
  ###################
  
  def <- defuzzifier(newdata, rule, object$degree.rule, range.output, names.fvaloutput, varout.mf, miu.rule, type.defuz, type.model, func.tsk)
  
  res <- list(rule = rule, varinp.mf = varinp.mf, varout.mf = varout.mf, y2 = MF, y3 = miu.rule, y3.indx = miu.rule.indx, y4 = def$y4, func.tsk = func.tsk, predicted.val = def$y5)
  
  return(res)
}