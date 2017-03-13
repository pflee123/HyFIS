## rule matrix to list
rulebase <- function(type.model, rule, func.tsk = NULL){
  if (class(rule) == "matrix"){
    rule.list <- list() 
    for (i in 1 : nrow(rule)){
      rule.list[i] <- list(rule[i, ])
    }
    rule <- rule.list	
  }
  
  
  ##condition for Mamdani model
  if (type.model == 1 || type.model == 2 || type.model == "MAMDANI" || type.model == "FRBCS" || type.model == "TSK") {
    ##Checking the sign of separating between antecedent and consequence 
    for (i in 1 : length(rule)){
      equal <- rule[i]
      equal <- as.matrix(equal)
      check <- any(equal == "->")
      if (check == FALSE )
        stop("rule must contain \"->\" as separating between antecendent and consequence") 			
    }
  }	
  
  #condition for Takagi Sugeno Kang model
  if (type.model == 2 || type.model == "TSK") {	
    if (is.null(func.tsk))
      stop("You are using Takagi Sugeno Kang model, so the consequent part must be linear equations, please insert their values")
  }
  
  return(rule)
}

#' @param num.varinput the number of dataset colunms
#' @param num.labels.input a vector containing the number of each varible's labels
# y2
layer2.operation <- function(data, num.varinput, num.labels.input, varinp.mf){
  
  ##count number of column of data
  ncol.data <- ncol(data)
  
  ##count number of column of varinp.mf (matrix used to build membership function) 
  ncol.var <- ncol(varinp.mf)
  
  ##Inisialitation matrix of Membership function
  MF <- matrix(0, nrow = nrow(data), ncol = ncol.var)
  
  ##check 
  if (ncol.data != num.varinput)
    stop("Data is not the same as the number of input variables")
  if (ncol.var != sum(num.labels.input))
    stop("the parameter of membership function is not the same with variable")
  
  ##h is index equal to number of data
  ##i is index for numbering variable
  ##j is index equal to number of varinp.mf column
  ##ii is used for counting how many iteration has been done in the following loop
  ##jj is used for keeping the iteration continueing to next index in varinp.mf
  
  cum.labels <- cumsum(c(0,num.labels.input))
  ##iterate as along number of data
  for (h in 1 : nrow(data)){
    ##iterate for each crisp value on each data 
    for (i in 1: ncol(data)){
      
      ##loop all column on varinp.mf
       
      ##checking for type 1: Gaussian
      ##parameter=(mean a, standard deviation b)
      ##a=varinp.mf[2,]
      ##b=varinp.mf[3,]
      ind <- cum.labels[i] + 1:num.labels.input[i]
      if (varinp.mf[1,ind[1]] == 1){
        temp <- exp(- (data[h, i] - varinp.mf[2, ind])^2 / varinp.mf[3, ind]^2)
      }
      
      ##save membership function on MF for each data		
      MF[h, ind] <- temp
      
    }
    
  }
  
  return(MF)
}

# y3
layer3.operation <-function(MF, rule, names.varinput, type.tnorm, ...){
  
  ##number of dataset rows
  nMF <- nrow(MF)
  num.input <- ncol(MF)
  
  ##the number of rules
  nrule <- nrow(rule)
  
  ##give names on each column for detecting which rule will be fired
  colnames(MF) <- c(names.varinput)
  
  ##Iteration for n data
  rule.antecede <- rule[,1:num.input]
  
  degree.rule <- t(apply(MF, 1, function(x, ...){
    degree.temp <- sweep(rule.antecede, 2, x, `*`)
    if(type.tnorm == "PRODUCT"){
      return(apply(degree.temp, 1, function(x){
        prod(x[x!=0])
      }))
    }
  }))
  
  colnames(degree.rule) <- paste("Rule",1:ncol(degree.rule),sep = "_")
  ## result 
  ## number of row is based on number of data.
  ## number of column is based on number of rule.
  return(list(degree.rule = degree.rule, input.indx = 1:num.input))
}

# y4
layer4.operation <- function(rule, degree.rule, input.indx, type.snorm, ...){
  
  rule.consequence <- rule[, -input.indx]
  degree.output <- t(apply(degree.rule, 1, function(x, ...){
    degree.output <- sweep(rule.consequence, 1, x, `*`)
    if(type.snorm == "SUM" || is.null(type.snorm)){
      return(apply(degree.output, 2, sum))
    }
  }))
  
  colnames(degree.output) <- colnames(rule.consequence)
  return(degree.output)
}

# y5
layer5.operation <- function(degree.output, varoutput.mf, defuzz.func, ...){
  means <- varoutput.mf[2,]
  variances <- varoutput.mf[3,]
  
  output <- apply(degree.output, 1, function(x){
    if(defuzz.func == "COG" || is.null(defuzz.func))
      sum(x * means * variances) / sum(x * variances)
  })
  
  return(output)
}
