HyFIS <- function(data.train, num.labels, max.iter = 10, step.size = 0.01, type.tnorm = "MIN", 
                  type.snorm = "MAX", type.defuz = "COG", type.implication.func = "ZADEH") {
  
  source('~/FusionProject/R Code/FUSION_HyFIS/HyFIS_Training.R')
  source('~/FusionProject/R Code/FUSION_HyFIS/ModelPrediction.R')
  	
  
  type.mf = "GAUSSIAN"
  range.data <- matrix(nrow = 2, ncol = ncol(data.train))
  range.data[1, ] <- 0
  range.data[2, ] <- 1
  
  mod <- WM.parallel(data.train, num.labels, type.mf, type.tnorm, type.implication.func)
  
  data.test <- as.matrix(data.train[, 1 : (ncol(data.train) - 1)])
  varout.mf <- mod$varout.mf
  names.varoutput <- colnames(varout.mf)
  rule <- mod$rule
  
  rule.temp <- mod$rule
  varinp.mf <- mod$varinp.mf
  degree.rule <- mod$degree.rule
  mod$method.type <- "HYFIS"
  mod$type.tnorm <- type.tnorm
  mod$type.snorm <- type.snorm
  mod$type.defuz <- type.defuz
  mod$type.model <- "MAMDANI"
  mod$func.tsk <- NULL
  class(mod) <- 'model'
  var.mf <- cbind(varinp.mf, varout.mf)
  var.mf.old <- cbind(varinp.mf, varout.mf)
  residuals.old <- 0
  
  ## create progress bar
  progressbar <- txtProgressBar(min = 0, max = max.iter, style = 3)
  for (iter in 1 : max.iter){
    
    print(mod$varinp.mf)
    print(mod$varout.mf)
    res <- predict.norm(mod, data.test, TRUE)
    y.pred <- res$predicted.val
    y.real <- data.train[, ncol(data.train)]
    
    residuals <- sqrt(mean((y.real - y.pred)^2))
    
    print(residuals)
    if (residuals <= 0.00001){
      print("The error is small enough")
      break
    } 
    
    for (i in 1 : nrow(data.test)){
      
      dt.i <- data.test[i, ,drop = FALSE]
      dt.train.i <- data.train[i, ,drop = FALSE]
      
      # res <- predict.norm(mod, dt.i, FALSE)
      def <- res$predicted.val[i,] ## y5
      y4 <- res$y4[i,]
      miu.rule <- res$y3[i,]
      MF <- res$y2[i,]
      miu.rule.indx <- res$y3.indx[i,]
      
      ## measure error 
      y.pred <- def
      y.real <- data.train[i, ncol(data.train)]
      
      residuals <- abs(y.real - y.pred)
      
      new.var.mf <- HyFIS.update(dt.train.i, def, rule, names.varoutput, var.mf, miu.rule,miu.rule.indx, num.labels, MF, step.size, degree.rule, y4)
      
      ## update parameters
      mod$varout.mf <- new.var.mf$varout.mf 
      mod$varinp.mf <- new.var.mf$varinp.mf 
      
      var.mf <- cbind(mod$varinp.mf, mod$varout.mf)
      
    }
    
    setTxtProgressBar(progressbar, iter)
  }
  close(progressbar)
  varinp.mf <- mod$varinp.mf
  varout.mf <- mod$varout.mf
  
  rule <- rule.temp	
  mod <- list(num.labels = num.labels, varout.mf = varout.mf, rule = rule, varinp.mf = varinp.mf, func.tsk = NULL, 
              degree.rule = degree.rule, rule.data.num = mod$rule.data.num, method.type = "HYFIS", 
              type.tnorm = type.tnorm, type.snorm = type.snorm, type.defuz = type.defuz, type.model = "MAMDANI",
              type.mf = "GAUSSIAN", type.implication.func = type.implication.func)
  
  return (mod)
}  

HyFIS.parallel <- function(data.train, num.labels, max.iter = 10, step.size = 0.01, type.tnorm = "MIN", 
                  type.snorm = "MAX", type.defuz = "COG", type.implication.func = "ZADEH") {
  
  source('~/FusionProject/R Code/FUSION_HyFIS/HyFIS_Training.R')
  source('~/FusionProject/R Code/FUSION_HyFIS/ModelPrediction.R')
  
  require(foreach)
  require(doParallel)
  require(parallel)
  require(doSNOW)
  
  type.mf = "GAUSSIAN"
  range.data <- matrix(nrow = 2, ncol = ncol(data.train))
  range.data[1, ] <- 0
  range.data[2, ] <- 1
  
  # valid.rate <- 0.99
  # data.validation <- as.matrix(data.train[(nrow(data.train)*valid.rate+1):nrow(data.train),])
  data.train <- as.matrix(data.train)
  
  mod <- WM.parallel(data.train, num.labels, type.mf, type.tnorm, type.implication.func)
  
  varout.mf <- mod$varout.mf
  names.varoutput <- colnames(varout.mf)
  rule <- mod$rule
  
  rule.temp <- mod$rule
  varinp.mf <- mod$varinp.mf
  degree.rule <- mod$degree.rule
  mod$method.type <- "HYFIS"
  mod$type.tnorm <- type.tnorm
  mod$type.snorm <- type.snorm
  mod$type.defuz <- type.defuz
  mod$type.model <- "MAMDANI"
  mod$func.tsk <- NULL
  class(mod) <- 'model'
  var.mf <- cbind(varinp.mf, varout.mf)
  var.mf.old <- cbind(varinp.mf, varout.mf)
  
  nthread <- 10
  combine <- function(old,new){
    if(new$error <= old$error)
      return(new)
    else return(old)
  }
  
  mod <- list(mod = mod, var.mf = var.mf)
  
  ## create progress bar
  progressbar <- txtProgressBar(min = 0, max = max.iter, style = 3)	
  
  print('start training...')
  for (iter in 1 : max.iter){
    
    cl <- makePSOCKcluster(detectCores())
    registerDoSNOW(cl)
    mod <- foreach(icount(nthread), .combine = combine, .inorder = FALSE, .export = c('predict.norm','fuzzifier','inference','defuzzifier','HyFIS.update')) %dopar%
    {
      source('~/FusionProject/R Code/FUSION_HyFIS/HyFIS_Training.R', local = TRUE)
      source('~/FusionProject/R Code/FUSION_HyFIS/ModelPrediction.R', local = TRUE)
      
      n.size <- floor(nrow(data.train)/nthread)
      
      
      data.sample <- data.train[sample(nrow(data.train),size = n.size),]
      mod.temp <- mod$mod
      var.mf.temp <- mod$var.mf
      
      for (i in 1 : nrow(data.sample)){

        dt.i <- data.sample[i,1 : (ncol(data.sample) - 1) ,drop = FALSE]
        dt.train.i <- data.sample[i, ,drop = FALSE]

        res <- predict.norm(mod.temp, dt.i,parallel = FALSE)
        def <- res$predicted.val
        miu.rule <- res$miu.rule
        MF <- res$MF

        
        ## measure error
        y.pred <- def
        y.real <- dt.train.i[,ncol(data.train)]

        residuals <- abs(y.real - y.pred)

        ## stoping criteria by RMSE
        if (residuals <= 0.00001){
          print('The training accuracy is good enough....')
          break
        }
        new.var.mf <- HyFIS.update.parallel(dt.train.i, def, rule, names.varoutput, var.mf, miu.rule, num.labels, MF, step.size, degree.rule)

        ## update parameters
        mod.temp$varout.mf <- new.var.mf$varout.mf
        mod.temp$varinp.mf <- new.var.mf$varinp.mf
        var.mf.temp <- cbind(mod.temp$varinp.mf, mod.temp$varout.mf)

      }
      
      res <- predict.norm(mod.temp, data.train[,1:(ncol(data.train) - 1)], parallel = FALSE)
      def <- res$predicted.val
      y.pred <- def
      y.real <- data.validation[,ncol(data.validation)]
      
      residuals <- (y.real - y.pred)
      RMSE <- sqrt(mean(residuals^2))
      
      return(list(mod = mod.temp, error = RMSE, var.mf = var.mf.temp))
    }
    
    
    stopCluster(cl)
    rm(cl)
    ## progress bar
    setTxtProgressBar(progressbar, iter)
  }
  
  close(progressbar)
  
  print(mod$error)
  
  mod <- mod$mod
  varinp.mf <- mod$varinp.mf
  varout.mf <- mod$varout.mf
  
  rule <- rule.temp	
  mod <- list(num.labels = num.labels, varout.mf = varout.mf, rule = rule, varinp.mf = varinp.mf, func.tsk = NULL, 
              degree.rule = degree.rule, rule.data.num = mod$rule.data.num, method.type = "HYFIS", 
              type.tnorm = type.tnorm, type.snorm = type.snorm, type.defuz = type.defuz, type.model = "MAMDANI",
              type.mf = "GAUSSIAN", type.implication.func = type.implication.func)
  
  return (mod)
}  
