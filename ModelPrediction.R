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
  
  MF <- layer2.operation(newdata, num.input, num.labels.input, input_membership)
  
  ncol.MF <- ncol(MF)
  colnames(MF) <- c(names.input)
  
  degree.rule <- layer3.operation(MF, rule, names.input, type.tnorm)
  
  input.indx <- degree.rule$input.indx
  degree.rule <- degree.rule$degree.rule
  degree.output <- layer4.operation(rule, degree.rule, input.indx, type.snorm)
  
  output <- layer5.operation(degree.output, output_membership, defuzz.func = type.defuz)
  
  res <- list(rule = rule, varinp.mf = input_membership, varout.mf = output_membership, y2 = MF, y3 = degree.rule, y4 = degree.output, predicted.val = output)
  
  return(res)
}