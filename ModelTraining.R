ModelCreation <- function(data.train, range.data = NULL, method.type = c("WM"), control=list(), inputId, conn, Input_Discription = NULL){
  require(jsonlite)
  source('~/FusionProject/R Code/FUSION_HyFIS/GeneralFunction.R')
  source('~/FusionProject/R Code/FUSION_HyFIS/FunctionForPrediction.R')
  source('~/FusionProject/R Code/FUSION_HyFIS/WM_Build.R')
  source('~/FusionProject/R Code/FUSION_HyFIS/HyFIS_Build.R')
  source('~/FusionProject/R Code/FUSION_HyFIS/InsertDatabase.R')
  source('~/FusionProject/R Code/FUSION_HyFIS/ModelTraining.R')
  
  jsonConfiguration <- serializeJSON(control)
  
  Model_Discription <- method.type #readline(prompt = paste0("Enter ",model.type," model description:"))
  model <- list(Type = method.type, Configuration = jsonConfiguration, Model_Discription = Model_Discription)
  
  method.type <- toupper(method.type)
  
  colnames.var <- colnames(data.train)
  
  mod <- NULL
  
  if(class(data.train) != "matrix"){
    data.train <- as.matrix(data.train)
  }
  
  if(is.null(range.data)){
    if(any(method.type == c("WM","HYFIS"))){
      range.data <- apply(data.train,2,range)
    }
  }
  
  if(any(method.type == c("WM","HYFIS"))){
    control <- setDefaultParametersIfMissing(control, list(num.labels = 7, max.iter = 10,step.size = 0.01, type.mf = "GAUSSIAN", type.defuz = "WAM", type.tnorm = "MIN",type.snorm = "MAX", type.implication.func = "ZADEH", name=method.type))
    
    ## get parameters
    range.data.ori <- range.data
    data.train.ori <- data.train
    num.labels <- control$num.labels
    type.mf <- control$type.mf
    name <- control$name
    type.tnorm <- control$type.tnorm
    type.snorm <- control$type.snorm
    type.defuz <- control$type.defuz
    type.implication.func <- control$type.implication.func
    ## replicate num of labels for each attributes
    num.labels <- matrix(rep(num.labels, ncol(data.train)), nrow=1)
    
    ## normalize data training
    data.tra.norm <- norm.data.parallel(data.train.ori, range.data.ori, min.scale = 0, max.scale = 1)
    
    if (method.type == "WM"){
      modelSpecific <- WM(data.tra.norm, num.labels, type.mf, type.tnorm, type.implication.func)
      
      ## collect results as model
      mod <- modelSpecific
      mod$type.model <- "MAMDANI"
      mod$func.tsk <- NULL
      mod$type.defuz <- type.defuz
      mod$type.snorm <- type.snorm
      mod$range.data.ori <- range.data.ori
    }else if(method.type == "HYFIS"){
      max.iter <- control$max.iter
      # step.size <- control$step.size
      step.size <- 0.001
      modelSpecific <- HyFIS(data.tra.norm, num.labels, max.iter, step.size, type.tnorm, type.snorm, type.defuz, type.implication.func)
      mod <- modelSpecific
      mod$range.data.ori <- range.data.ori
    }
  }else
    stop("The model name does not match the existing models...")
  
  ## generate FRBS model
  mod$method.type <- method.type
  mod$name <- name
  mod <- normalize.params(mod)
  
  ##colnames.var
  if (!is.null(colnames.var)) {
    mod$colnames.var <- colnames.var 
  } else {
    mod$colnames.var <- paste("var", seq(1, ncol(data.train)), sep = ".")
  }
  
  class(mod) <- "model"
  
  # if (!is.null(mod$rule) && !is.null(mod$num.labels)){
  #   mod$rule <- rep.rule(mod)
  # }
  jsonObject <- serializeJSON(mod)
  
  Experiment_Reason <- sprintf("Using %s and %s to predict", model.type, Input_Discription)#readline(prompt = "Enter this experiment season:")
  experiment <- list(Training_Parameters = jsonObject, Experiment_Reason = Experiment_Reason)
  
  return(list(object = mod, experiment = experiment, model = model, InputId = inputId))
}

setDefaultParametersIfMissing <- function(control, defaults) {
  for(i in names(defaults)) {
    if(is.null(control[[i]])) 
      control[[i]] <- defaults[[i]]
  }
  
  return(control)
}

normalize.params <- function(mod){
  ## define a collection of values of parameters
  type.tnorm.str <- c("MIN", "HAMACHER", "YAGER", "PRODUCT", "BOUNDED")
  type.snorm.str <- c("MAX", "HAMACHER", "YAGER", "SUM", "BOUNDED")
  type.defuz.str <- c("WAM", "FIRST.MAX", "LAST.MAX", "MEAN.MAX", "COG")
  type.mf.str <- c("TRIANGLE", "TRAPEZOID", "GAUSSIAN", "SIGMOID", "BELL")
  type.model.str <- c("MAMDANI", "TSK", "FRBCS", "CLUSTERING", "APPROXIMATE")
  
  if (any(mod$type.model == c("MAMDANI", "TSK", "FRBCS", "APPROXIMATE"))){
    
    ## check type.tnorm
    if (is.null(mod$type.tnorm)) {
      warning("type.tnorm is not defined, it will be assigned to 'MIN' ")
      mod$type.tnorm <- "MIN"
    }
    else if (class(mod$type.tnorm) == "numeric")
      mod$type.tnorm <- type.tnorm.str[mod$type.tnorm]
    else if (class(mod$type.tnorm) == "character")
      mod$type.tnorm <- toupper(mod$type.tnorm)
    
    ## check type.snorm
    if (is.null(mod$type.snorm)){
      warning("type.snorm is not defined, it will be assigned to 'MAX' ")
      mod$type.snorm <- "MAX"
    }
    else if (class(mod$type.snorm) == "numeric")
      mod$type.snorm <- type.snorm.str[mod$type.snorm]
    else if (class(mod$type.snorm) == "character")
      mod$type.snorm <- toupper(mod$type.snorm)
    
    ## check type.implication.func
    mod$type.implication.func <- toupper(mod$type.implication.func)
  }
  
  
  ## check type.defuz
  if (!is.null(mod$type.defuz) && class(mod$type.defuz) == "numeric")
    mod$type.defuz <- type.defuz.str[mod$type.defuz]
  else if (!is.null(mod$type.defuz) && class(mod$type.defuz) == "character")
    mod$type.defuz <- toupper(mod$type.defuz)
  
  ## check type.mf
  if (!is.null(mod$type.mf) && class(mod$type.mf) == "numeric")
    mod$type.mf <- type.mf.str[mod$type.mf]
  else if (!is.null(mod$type.mf) && class(mod$type.mf) == "character")
    mod$type.mf <- toupper(mod$type.mf)
  
  ## check type.model
  if (is.null(mod$type.model))
    stop("please define type of model")
  else if (class(mod$type.model) == "numeric")
    mod$type.model <- type.model.str[mod$type.model]
  else if (class(mod$type.model) == "character")
    mod$type.model <- toupper(mod$type.model)
  
  ## check type.defuz and MAMDANI
  if (mod$type.model == "MAMDANI" && is.null(mod$type.defuz)){
    warning("type.defuz is not defined, it will be assigned to 'WAM' ")
    mod$type.defuz <- "WAM"
  }
  
  return (mod)
}

rep.rule <- function(object){
  
  ## get names of variables
  colnames.var <- object$colnames.var
  
  ## make description on rule
  if (any(object$method.type == c("WM"))){
    
    ## get number of input variables and number of linguistic terms	
    num.varinput <- length(colnames.var) - 1
    num.labels <- object$num.labels
    
    ## get names of linguistic terms
    if (!is.null(object$varinp.mf)){
      names.fTerms.inpvar <- colnames(object$varinp.mf)
    } else {names.fTerms.inpvar = NULL	}
    if (!is.null(object$varout.mf)){
      names.fTerms.outvar <- colnames(object$varout.mf)
    } else {names.fTerms.outvar = NULL }
    
    ##	construct rule from rule.data.num (or rule in matrix format).
    if (is.null(object$rule)&&!is.null(object$rule.data.num)){
      res <- generate.rule(object$rule.data.num, num.labels,names.fTerms.inpvar = names.fTerms.inpvar, names.fTerms.outvar = names.fTerms.outvar)
      rule <- res$rule
    } else {
      rule <- object$rule
    }
    
    ## construct rule in IF ... THEN ... format
    new.rule <- matrix(nrow = nrow(rule), ncol = (ncol(rule) + 2 * (num.varinput + 1)))
    k <- 1
    
    for (j in 1 : num.varinput){				
      new.rule[, k] <- colnames.var[j] 
      new.rule[, k + 1] <- "is"
      new.rule[, k + 2] <- rule[, 2 * j - 1]
      
      if (j < num.varinput){
        ## new.rule[, k + 3] <- "and"
        ## A bug: when the boolean operator "or" (solved)
        new.rule[, k + 3] <- rule[, 2 * j]
      } else {
        new.rule[, k + 3] <- "THEN"
      }
      k <- k + 4
    }
    new.rule[, (ncol(new.rule) - 2)] <- colnames.var[num.varinput + 1] 
    new.rule[, (ncol(new.rule) - 1)] <- "is"
    new.rule[, ncol(new.rule)] <- rule[, ncol(rule)]
    
    if(object$type.model == c("MAMDANI")){
      rule <- new.rule
    }
    
    rule <- cbind("IF", rule)
  } else 
    stop("It is not supported to create rule representation")
  
  return (rule)
} 

generate.rule <- function(rule.data.num, num.labels, names.fTerms.inpvar = NULL, names.fTerms.outvar = NULL){
  
  ## build the names of linguistic values	
  if (is.null(names.fTerms.inpvar) || is.null(names.fTerms.outvar)){
    fuzzyTerm <- create.fuzzyTerm(classification = FALSE, num.labels)
    names.inp.var <- fuzzyTerm$names.fvalinput
    names.out.var <- fuzzyTerm$names.fvaloutput
    names.variable <- c(names.inp.var, names.out.var)
  }
  else {
    names.inp.var <- names.fTerms.inpvar
    names.out.var <- names.fTerms.outvar
    names.variable <- c(names.inp.var, names.out.var)
  }
  
  ## build the rule into list of string
  rule <- matrix(nrow = nrow(rule.data.num), ncol = 2 * ncol(rule.data.num) - 1)
  
  for (i in 1 : nrow(rule.data.num)){
    k <- 0
    for (j in 1 : ncol(rule.data.num)){
      k <- k + 1	
      if (j == ncol(rule.data.num) - 1){
        if (rule.data.num[i, j] == 0){
          rule[i, k] <- c("dont_care")
        } else {
          rule[i, k] <- c(names.variable[rule.data.num[i, j]])
        }
        rule[i, k + 1] <- c("->")
        k <- k + 1
      }
      else if (j == ncol(rule.data.num)){
        if (rule.data.num[i, j] == 0){
          rule[i, k] <- c("dont_care")
        } else {
          rule[i, k] <- c(names.variable[rule.data.num[i, j]])
        }
      }
      else{
        if (rule.data.num[i, j] == 0){
          rule[i, k] <- c("dont_care")
        } else {
          rule[i, k] <- c(names.variable[rule.data.num[i, j]])
        }
        rule[i, k + 1] <- c("and")
        k <- k + 1
      }
    }
    
  }
  res <- list(rule = rule, names.varinput = names.inp.var, names.varoutput = names.out.var)
  return (res)
}

## ineffient
generate.rule.parallel <- function(rule.data.num, num.labels, names.fTerms.inpvar = NULL, names.fTerms.outvar = NULL){
  
  require(foreach)
  require(doParallel)
  
  cl <- makeCluster(4)
  registerDoParallel(cl)
  
  ## build the names of linguistic values	
  if (is.null(names.fTerms.inpvar) || is.null(names.fTerms.outvar)){
    fuzzyTerm <- create.fuzzyTerm(classification = FALSE, num.labels)
    names.inp.var <- fuzzyTerm$names.fvalinput
    names.out.var <- fuzzyTerm$names.fvaloutput
    names.variable <- c(names.inp.var, names.out.var)
  }
  else {
    names.inp.var <- names.fTerms.inpvar
    names.out.var <- names.fTerms.outvar
    names.variable <- c(names.inp.var, names.out.var)
  }
  
  ## build the rule into list of string
  rule <- matrix(nrow = nrow(rule.data.num), ncol = 2 * ncol(rule.data.num) - 1)
  
  rule <- foreach (i = 1 : nrow(rule.data.num), .combine = rbind) %dopar%
  {
    k <- 0
    for (j in 1 : ncol(rule.data.num)){
      k <- k + 1	
      if (j == ncol(rule.data.num) - 1){
        if (rule.data.num[i, j] == 0){
          rule[i, k] <- c("dont_care")
        } else {
          rule[i, k] <- c(names.variable[rule.data.num[i, j]])
        }
        rule[i, k + 1] <- c("->")
        k <- k + 1
      }
      else if (j == ncol(rule.data.num)){
        if (rule.data.num[i, j] == 0){
          rule[i, k] <- c("dont_care")
        } else {
          rule[i, k] <- c(names.variable[rule.data.num[i, j]])
        }
      }
      else{
        if (rule.data.num[i, j] == 0){
          rule[i, k] <- c("dont_care")
        } else {
          rule[i, k] <- c(names.variable[rule.data.num[i, j]])
        }
        rule[i, k + 1] <- c("and")
        k <- k + 1
      }
    }
    
    return(rule[i,])
  }
  
  stopCluster(cl)
  res <- list(rule = rule, names.varinput = names.inp.var, names.varoutput = names.out.var)
  return (res)
}

create.fuzzyTerm <- function(classification = FALSE, num.labels){
  
  # number of input variable
  num.inpvar <- (ncol(num.labels) - 1)
  num.inp.var <- sum(num.labels[1, 1 : (ncol(num.labels) - 1)])
  seq.inp.num <- seq(from = 1, to = num.inp.var, by = 1)
  temp <- list()
  k <- 0
  for (i in 1 : num.inpvar){
    num.label <- num.labels[1,i]
    for(j in 1:num.label){
      var <- paste("v", i, sep = ".")
      
      fuz <- paste("a", j, sep = ".")				
      new <- paste(var, fuz, sep ="_")
      temp <- append(temp, new)
    }
  }
  
  fuzzy.labels.inpvar <- as.character(temp)
  
  num.out.var <- num.labels[1, ncol(num.labels)]
  seq.out.num <- seq(from = 1, to = num.out.var, by = 1)
  fuzzy.labels.outvar <- paste("c", seq.out.num, sep = ".")
  
  return(list(names.fvalinput = fuzzy.labels.inpvar, names.fvaloutput = fuzzy.labels.outvar))
}