HyFIS.update <- function(data.train, def, rule, names.varoutput, var.mf, miu.rule, miu.rule.indx, num.labels, MF, step.size= 0.001, degree.rule, y4){
  
  rule.temp <- rule
  
  indx <- length(num.labels)
  l.output <- (ncol(var.mf) - num.labels[indx])
  
  data <- data.train
  
  ## get varinp.mf
  varinp.mf <- var.mf[, 1 : l.output, drop = FALSE]
  
  ## get varout.mf
  varout.mf <- var.mf[, (l.output + 1) : ncol(var.mf), drop = FALSE]
  
  ##Check not zero on miu.rule
  
  l.chck <- length(which(y4 >0)) # number of relative rules
  
  #if there are some no zero element on miu rule
  if (l.chck != 0) {
    
    y5 <- def
    y4 <- y4
    y3 <- miu.rule
    y2 <- MF
    
    #########Ouput variables############
    ## Update center(mean) and width(variance) on output variable (layer 5)
    
    ## output of eta -- learning rate
    eta.o <- step.size
    ## input of eta -- learning rate
    eta.i <- step.size
    delta.layer5 <- (def - data[, ncol(data)]) ## delta of layer 5 --> E/y5
    
    # ss <- seq(from = 1, to = num.labels[1, ncol(num.labels)])
    # 
    # indx.comp <- cbind(ss, 0)
    # for (i in 1:nrow(indx)){
    #   indx.comp[indx[i, 1], 2] <- (indx[i, 2] * indx[i, 3]) ## y4 each label of output
    # }
    
    mean <- varout.mf[2, ]
    variance <- varout.mf[3, ]
    delta.layer4 <- matrix(0,nrow = 1, ncol = length(names.varoutput))
    
    ## update output variable
    for (i in 1:ncol(varout.mf)){
      
      if (sum(y4 * variance) != Inf) {
        ## y5/var
        delta.var <- y4[i] * (mean[i] * sum(y4 * variance) - sum(y4 * mean * variance)) /(sum(y4 * variance))^2
        ## E/var -> E/y5 * y5/var
        temp.var <- delta.layer5 * delta.var
        varout.mf[3, i] <- varout.mf[3, i] + eta.o * temp.var
        
        ## y5/c
        delta.mean <- y4[i] * variance[i] / (sum(variance * y4))^ 2
        ## E/c -> E/y5 * y5/c
        temp.mean <- delta.layer5 * delta.mean
        varout.mf[2, i] <- varout.mf[2, i] + eta.o * temp.mean
      }
      
      ## y5/y4
      temp.y4 <- variance[i]*(mean[i] * sum(y4 * variance) - sum(y4 * mean * variance)) /(sum(y4 * variance))^2
      delta.layer4[i] <- temp.y4 * delta.layer5
      
      if (varout.mf[2, i] < 0)
        varout.mf[2, i] <- 0
      if (varout.mf[3, i] <= 0)
        varout.mf[3, i] <- 0.001
      
      if (varout.mf[2, i] > 1)
        varout.mf[2, i] <- 1
      if (varout.mf[3, i] > 1)
        varout.mf[3, i] <- 1
      
    }
    
    delta.layer2 <- sum(delta.layer4)
    
    ##########Input variables############
    ## Update center(mean) and width(variance) on input variable (layer 3)
    
    for (j in colnames(varinp.mf)){
      
      indx.r <- which(j == miu.rule.indx)
      
      if(length(indx.r) > 0){
          ## E/y2
          x <- data.train[1, na.omit(as.numeric(unlist(strsplit(j, "[^0-9]+"))))[1]]
          a <- y2[j] * 2 *(x -  varinp.mf[2, j])/(varinp.mf[3, j])^2	
          if(!is.na(a))
            varinp.mf[2, j] <- varinp.mf[2, j] + eta.i * delta.layer2 * a
          b <- y2[j] * 2 *(x -  varinp.mf[2, j])^2/(varinp.mf[3, j])^3
          if(!is.na(b))
            varinp.mf[3, j] <- varinp.mf[3, j] + eta.i * delta.layer2 * b

          if (varinp.mf[2, j] < 0)
            varinp.mf[2, j] <- 0
          if (varinp.mf[3, j] <= 0)
            varinp.mf[3, j] <- 0.001

          if (varinp.mf[2, j] > 1)
            varinp.mf[2, j] <- 1
          if (varinp.mf[3, j] > 1)
            varinp.mf[3, j] <- 1
      }
      
    }
    
  }
  var.mf <- list(varinp.mf = varinp.mf, varout.mf = varout.mf)
  
  return(var.mf)
  
}

HyFIS.update.original <- function(data.train, def, rule, names.varoutput, var.mf, miu.rule, num.labels, MF, step.size = 0.001){
  
  rule.temp <- rule
  
  indx <- length(num.labels)
  l.output <- (ncol(var.mf) - num.labels[indx])
  
  data <- data.train
  
  ## get varinp.mf
  varinp.mf <- var.mf[, 1 : l.output, drop = FALSE]
  
  ## get varout.mf
  varout.mf <- var.mf[, (l.output + 1) : ncol(var.mf), drop = FALSE]
  
  ##Check not zero on miu.rule
  chck <- which(miu.rule > 0.00001)
  l.chck <- length(chck) # number of relative rules
  
  #if there are some no zero element on miu rule
  if (l.chck != 0) {
    indx <- matrix(nrow = l.chck, ncol = 3)
    temp.indx <- matrix(nrow = 1, ncol = 3)
    
    ## along number of not zero on miu.rule, check and save string related on names.varoutput and its value
    for (ii in 1 : l.chck){
      #get the names of output value on each rule
      strg <- c(rule.temp[chck[ii], ncol(rule.temp)])	
      aaa <- which(strg == names.varoutput)
      
      if (length(aaa) != 0) {
        indx[ii, 1] <- aaa
        indx[ii, 2] <- miu.rule[chck[ii]] ## y3
        indx[ii, 3] <- degree.rule[chck[ii]] ## weights
      }
    }
    
    ## check duplication on indx, choose with has a max of degree
    indx.temp <- cbind(indx, (indx[, 2] * indx[, 3] ^ 2)) ## y4
    
    if (nrow(indx.temp) < 2){
      indx <- indx.temp
    }
    else {
      indx.temp <- indx.temp[order(indx.temp[,4], decreasing = TRUE),]
      indx.nondup <- which(duplicated(indx.temp[, 1, drop=FALSE]) == FALSE, arr.ind = TRUE) ## delete the duplicate output labels and find the maximum degree of rules	
      indx.temp <- indx.temp[indx.nondup[,1], ,drop = FALSE] 
      indx <- indx.temp
    }
    
    ## y4    
    
    #####################
    ## Update center(mean) and width(variance) on output variable (layer 5)
    ####################
    
    eta.o <- step.size
    eta.i <- step.size
    delta <- (data.train[1, ncol(data.train)] - def) ## delta of layer 5
    
    ss <- seq(from = 1, to = num.labels[1, ncol(num.labels)])
    
    indx.comp <- cbind(ss, 0)
    for (i in 1:nrow(indx)){
      indx.comp[indx[i, 1], 2] <- (indx[i, 2] * indx[i, 3]) ## y4 each label of output
    }
    
    mean.t <- t(t(varout.mf[2, ]))
    var.t <- t(t(varout.mf[3, ]))
    
    ###update output variable
    delta.k <- 0
    for (i in 1:ncol(varout.mf)){
      
      if (sum(var.t * indx.comp[, 2], na.rm = TRUE) != Inf && is.na(varout.mf[2, i]) && is.na(varout.mf[3, i])) {
        delta.k4 <-  delta * varout.mf[3, i] * (varout.mf[2, i] * sum(indx.comp[, 2] * var.t) - sum(indx.comp[, 2] * mean.t * var.t))* (1 / (sum(indx.comp[, 2] * var.t))^2)
        temp <- delta * indx.comp[i, 2] * (varout.mf[2, i] * sum(indx.comp[, 2] * var.t) - sum(indx.comp[, 2] * mean.t * var.t))* (1 / (sum(indx.comp[, 2] * var.t))^2)
        varout.mf[3, i] <- varout.mf[3, i] + eta.o * temp
        varout.mf[2, i] <- varout.mf[2, i] + eta.o * delta * (varout.mf[2, i] * indx.comp[i, 2]) / (sum(var.t * indx.comp[, 2]))
      } else {
        delta.k4 <-  0
      }
      
      delta.k <- delta.k + delta.k4
      if (varout.mf[2, i] < 0)
        varout.mf[2, i] <- 0
      if (varout.mf[3, i] <= 0)
        varout.mf[3, i] <- 0.001
      
      if (varout.mf[2, i] > 1)
        varout.mf[2, i] <- 1
      if (varout.mf[3, i] > 1)
        varout.mf[3, i] <- 1
      
    }
    
    ######################
    ## Update center(mean) and width(variance) on input variable (layer 3)
    ######################
    
    num.varinput <- ncol(data.train) - 1
    for (j in 0 : (num.varinput - 1)){
      
      
      start.v <- j * num.labels[1, j + 1] + 1
      end.v <- start.v + num.labels[1, j + 1] - 1
      term.min <- which.min(MF[start.v : end.v]) 
      
      ## get data correspondent with input variable
      indxx <- j * num.labels[1, j + 1] + term.min
      
      ################
      a <- MF[indxx] * 2 *(data.train[1, j  + 1] -  varinp.mf[2, indxx])/(varinp.mf[3, indxx])^2		
      varinp.mf[2, indxx] <- varinp.mf[2, indxx] + eta.i * delta.k * a * delta	
      b <- MF[indxx] * 2 *(data.train[1, j + 1] -  varinp.mf[2, indxx])^2/(varinp.mf[3, indxx])^3	
      varinp.mf[3, indxx] <- varinp.mf[3, indxx] + eta.i * delta.k * b * delta
      
      if (varinp.mf[2, indxx] < 0)
        varinp.mf[2, indxx] <- 0
      if (varinp.mf[3, indxx] < 0)
        varinp.mf[3, indxx] <- 0.001
      
      if (varinp.mf[2, indxx] > 1)
        varinp.mf[2, indxx] <- 1
      if (varinp.mf[3, indxx] > 1)
        varinp.mf[3, indxx] <- 1
    }	
  }
  var.mf <- list(varinp.mf = varinp.mf, varout.mf = varout.mf)
  
  return(var.mf)
  
}

HyFIS.update.parallel <- function(data.train, def, rule, names.varoutput, var.mf, miu.rule, miu.rule.indx, num.labels, MF, step.size= 0.001, degree.rule, y4){
  
  rule.temp <- rule
  
  indx <- length(num.labels)
  l.output <- (ncol(var.mf) - num.labels[indx])
  
  data <- data.train
  
  ## get varinp.mf
  varinp.mf <- var.mf[, 1 : l.output, drop = FALSE]
  
  ## get varout.mf
  varout.mf <- var.mf[, (l.output + 1) : ncol(var.mf), drop = FALSE]
  
  ##Check not zero on miu.rule
  chck <- which(miu.rule[1, ] > 0.0001)
  l.chck <- length(chck)
  
  #if there are some no zero element on miu rule
  if (l.chck != 0) {
    indx <- matrix(nrow = l.chck, ncol = 3)
    temp.indx <- matrix(nrow = 1, ncol = 3)
    
    ## along number of not zero on miu.rule, check and save string related on names.varoutput and its value
    for (ii in 1 : l.chck){
      #get the names of output value on each rule
      strg <- c(rule.temp[chck[ii], ncol(rule.temp)])		
      aaa <- which(strg == names.varoutput)
      
      if (length(aaa) != 0) {
        indx[ii, 1] <- aaa
        indx[ii, 2] <- miu.rule[chck[ii]]
        indx[ii, 3] <- degree.rule[chck[ii]]
      }
    }
    
    ## check duplication on indx, choose with has a max of degree
    indx.temp <- cbind(indx, (indx[, 2] * indx[, 3]))
    if (nrow(indx.temp) < 2){
      indx <- indx.temp
    }else {
      indx.temp <- indx.temp[order(indx.temp[,c(4)], decreasing = TRUE),]
      indx.nondup <- which(duplicated(indx.temp[, 1, drop=FALSE]) == FALSE, arr.ind = TRUE)	
      indx.temp <- indx.temp[indx.nondup, ,drop = FALSE] 
      indx <- indx.temp[, -4]
    }
    
    #####################
    ## Update center(mean) and width(variance) on output variable (layer 5)
    ####################
    
    eta.o <- step.size
    eta.i <- step.size
    delta <- (data.train[1, ncol(data.train)] - def) # delta of layer 5(output) -- 1
    
    ss <- seq(from = 1, to = num.labels[1, ncol(num.labels)])
    
    indx.comp <- cbind(ss, 0)
    for (i in 1:nrow(indx)){
      indx.comp[indx[i, 1], 2] <- (indx[i, 2] * indx[i, 3])
    }
    
    mean.t <- t(t(varout.mf[2, ]))
    var.t <- t(t(varout.mf[3, ]))
    
    ###update output variable
    delta.4 <- vector(mode = "numeric", length = ncol(varout.mf)) # delta of layer 4(output label) -- number of output label
    for (i in 1:ncol(varout.mf)){
      
      if (sum(var.t * indx.comp[, 2]) != Inf) {
        delta.k4 <-  delta * varout.mf[3, i] * (varout.mf[2, i] * sum(indx.comp[, 2] * var.t) - sum(indx.comp[, 2] * mean.t * var.t))* (1 / (sum(indx.comp[, 2] * var.t))^2) # delta of layer 4
        temp <-  delta * indx.comp[i, 2] * (varout.mf[2, i] * sum(indx.comp[, 2] * var.t) - sum(indx.comp[, 2] * mean.t * var.t))* (1 / (sum(indx.comp[, 2] * var.t))^2) # gradient of layer 5/ var
        varout.mf[3, i] <- varout.mf[3, i] + eta.o * delta * temp
        varout.mf[2, i] <- varout.mf[2, i] + eta.o * delta * (varout.mf[2, i] * indx.comp[i, 2]) / (sum(var.t * indx.comp[, 2]))
      } else {
        delta.k4 <-  0
      }
      
      delta.4[i] <- delta * delta.k4
      if (varout.mf[2, i] < 0)
        varout.mf[2, i] <- 0
      if (varout.mf[3, i] < 0)
        varout.mf[3, i] <- 0.001
      
      if (varout.mf[2, i] > 1)
        varout.mf[2, i] <- 1
      if (varout.mf[3, i] > 1)
        varout.mf[3, i] <- 1
      
    }
    
    ## layer 4 --> delta.4 -- number of output label
    ## layer 3 = delta.4 -- number of rules
    delta.3 <- vector(mode = "numeric", length = nrow(rule.temp))
    for(j in 1:nrow(rule.temp)){
      index.rule <- which(names.varoutput == rule.temp[j,ncol(rule.temp)])
      delta.3[j] <- delta.4[index.rule]
    }
    
    ######################
    ## Update center(mean) and width(variance) on input variable (layer 2)
    ######################
    names.varinput <- colnames(varinp.mf)
    ## the gradient of layer 2
    delta.2 <- 0
    num.varinput <- ncol(data.train) - 1
    for(i in 1:nrow(rule.temp)){
      
      delta.2 <- delta.2 + delta.3[i]
    }
    
    for (j in 0 : (num.varinput - 1)){
      
      start.v <- j * num.labels[1, j + 1] + 1
      end.v <- start.v + num.labels[1, j + 1] - 1
      term.min <- which.min(MF[1, start.v : end.v]) 
      
      ## get data correspondent with input variable
      indxx <- j * num.labels[1, j + 1] + term.min
      
      ################
      a <- MF[1, indxx] * 2 *(data.train[1, j  + 1] -  varinp.mf[2, indxx])/(varinp.mf[3, indxx])^2		
      varinp.mf[2, indxx] <- varinp.mf[2, indxx] + eta.i * delta.2 * a
      b <- MF[1, indxx] * 2 *(data.train[1, j + 1] -  varinp.mf[2, indxx])^2/(varinp.mf[3, indxx])^3	
      varinp.mf[3, indxx] <- varinp.mf[3, indxx] + eta.i * delta.2 * b
      # if(indxx %% 5 != 0 && indxx %% 5 != 1)
      #   cat(indxx,';',eta.i * delta.2 * a,';',eta.i * delta.2 * b,'\n')
      
      if (varinp.mf[2, indxx] < 0)
        varinp.mf[2, indxx] <- 0
      if (varinp.mf[3, indxx] < 0)
        varinp.mf[3, indxx] <- 0.001
      
      if (varinp.mf[2, indxx] > 1)
        varinp.mf[2, indxx] <- 1
      if (varinp.mf[3, indxx] > 1)
        varinp.mf[3, indxx] <- 1
    }
  }
  var.mf <- list(varinp.mf = varinp.mf, varout.mf = varout.mf)
  return(var.mf)
  
}
