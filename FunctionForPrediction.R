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

fuzzifier <- function(data, num.varinput, num.labels.input, varinp.mf){
  
  ##count number of column of data
  ncol.data <- ncol(data)
  
  ##count number of column of varinp.mf (matrix used to build membership function) 
  ncol.var <- ncol(varinp.mf)
  
  ##Inisialitation matrix of Membership function
  MF <- matrix(nrow = nrow(data), ncol = ncol.var)
  
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

fuzzifier.parallel <- function(data, num.varinput, num.labels.input, varinp.mf){
  
  require(foreach)
  require(doParallel)
  require(doSNOW)
  require(parallel)
  
  cl <- makePSOCKcluster(detectCores())
  registerDoSNOW(cl)
  ##count number of column of data
  ncol.data <- ncol(data)
  
  ##count number of column of varinp.mf (matrix used to build membership function) 
  ncol.var <- ncol(varinp.mf)
  
  ##Inisialitation matrix of Membership function
  MF <- matrix(nrow = nrow(data), ncol = ncol.var)
  VF <- matrix(nrow = 1, ncol = ncol.var)
  
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
  
  ##iterate as along number of data
  MF <- foreach (h = 1 : nrow(data), .combine = rbind) %dopar%
  {
    jj <- 1
    ##iterate for each crisp value on each data 
    for (i in 1: ncol(data)){
      
      ##counter for break
      ii <- 1
      
      ##loop all column on varinp.mf
      for (j in jj : ncol(varinp.mf)){		
        
        ##
        ##checking for type 1: Triangular, if varinp.mf[1,] == 1 
        ##parameter=(a,b,c), where a < b < c
        ##a=varinp.mf[2,]
        ##b=varinp.mf[3,]
        ##c=varinp.mf[4,]
        if (varinp.mf[1, j] == 1){
          if (data[h, i] <= varinp.mf[2, j]) temp <- 0
          else if (data[h, i] <= varinp.mf[3, j]) temp <- (data[h, i] - varinp.mf[2, j]) / (varinp.mf[3, j] - varinp.mf[2, j])
          else if (data[h, i] < varinp.mf[4, j]) temp <- (data[h, i] - varinp.mf[4, j]) / (varinp.mf[3, j] - varinp.mf[4, j])
          else temp <- 0
        }
        
        ##checking for type 2: Trapezoid_1a, if varinp.mf[1,] ==2
        ##Trapezoid_1a is the edge on the left: vertical
        ##parameter=(a,b,c)
        ##a=varinp.mf[2,]
        ##b=varinp.mf[3,]
        ##c=varinp.mf[4,]
        else if (varinp.mf[1, j] == 2){
          if (data[h, i] <= varinp.mf[3, j]) temp <- 1
          else if (data[h, i] <= varinp.mf[4, j]) temp <- (data[h, i] - varinp.mf[4, j]) / (varinp.mf[3, j] - varinp.mf[4, j])
          else temp <- 0				
        }
        
        ##checking for type 3: Trapezoid_1b, if varinp.mf[1,] == 3
        ##Trapezoid_1b is the edge on the right: vertical
        ##parameter=(a,b,c)
        ##a=varinp.mf[2,]
        ##b=varinp.mf[3,]
        ##c=varinp.mf[4,]
        else if (varinp.mf[1, j] == 3){
          if (data[h, i] <= varinp.mf[2, j]) temp <- 0
          else if (data[h, i] < varinp.mf[3, j]) temp <- (data[h, i] - varinp.mf[2, j]) / (varinp.mf[3, j] - varinp.mf[2, j])
          else temp <- 1
        }
        
        ##checking for type 4: Trapezoid_2
        ##parameter=(a,b,c,d)
        ##a=varinp.mf[2,]
        ##b=varinp.mf[3,]
        ##c=varinp.mf[4,]
        ##d=varinp.mf[5,]
        else if (varinp.mf[1, j] == 4){
          if (data[h,i] <= varinp.mf[2, j] || data[h,i] > varinp.mf[5, j]) temp <- 0
          else if (data[h, i] > varinp.mf[3, j] && data[h, i] <= varinp.mf[4, j]) temp <- 1
          else if (data[h, i] > varinp.mf[2, j] && data[h, i] <= varinp.mf[3, j]) temp <- (data[h, i] - varinp.mf[2, j]) / (varinp.mf[3, j] - varinp.mf[2, j])
          else if (data[h,i] > varinp.mf[4, j] && data[h,i] <= varinp.mf[5,j]) temp <- (data[h, i] - varinp.mf[5, j]) / (varinp.mf[4, j] - varinp.mf[5, j])
        }
        
        ##checking for type 5: Gaussian
        ##parameter=(mean a, standard deviation b)
        ##a=varinp.mf[2,]
        ##b=varinp.mf[3,]
        else if (varinp.mf[1, j] == 5){
          temp <- exp(- (data[h, i] - varinp.mf[2, j])^2 / varinp.mf[3, j]^2)
        }
        
        ##checking for type 6: Sigmoid/logistic
        ##parameter=(gamma,c)
        ##gamma=varinp.mf[2,]
        ##c=varinp.mf[3,]
        else if (varinp.mf[1, j] == 6) {
          temp <- 1/(1 + exp(- varinp.mf[2, j] * (data[h, i] - varinp.mf[3, j])))
        }
        
        ##checking for type 7: Generalized Bell
        ##parameter=(a,b,c)
        ##a=varinp.mf[2,]
        ##b=varinp.mf[3,]
        ##c=varinp.mf[4,]
        else if (varinp.mf[1, j] == 7) {
          temp <- 1/(1 + abs((data[h, i] - varinp.mf[4, j])/varinp.mf[2, j]) ^ (2 * varinp.mf[3, j]))   
        }
        
        ##save membership function on MF for each data		
        VF[, j] <- temp
        
        ii <- ii + 1
        jj <- jj + 1
        ##this checking is used for control the number of linguistic value for each variable
        
        if (ii > num.labels.input[1, i])
          break
        
      }
    }
    
    VF
  }
  
  stopCluster(cl)
  return(MF)
}

inference<-function(MF, rule, names.varinput, type.tnorm, type.snorm){
  
  ##calculate number of data
  nMF <- nrow(MF)
  
  ##calculate number of rule
  nrule <- nrow(rule)
  
  ##allocate memory for membership function on antecedent
  miu.rule <- matrix(nrow = nMF, ncol = nrule)
  miu.rule.indx <- matrix(nrow = nMF, ncol = nrule)
  
  ##give names on each column for detecting which rule will be fired
  colnames(MF) <- c(names.varinput)
  
  ##Iteration for n data
  for(k in 1 : nMF){
    ##Iteration for each rule
    for(i in 1 : nrule){
      
      ##change list of rule into matrix
      temp <- rule[i,]
      
      ##detect location of "->" sign as separation between antecedent and consequence
      loc <- which(temp == "->")
      
      ##Inisialization for calculating MF of antecendet
      val.antecedent <- MF[k, temp[1]]
      min.indx <- temp[1]
      ## iterate to calculate degree of MF until find "->" sign (equals to loc)
      seqq <- seq(from = 1, to = loc, by = 2)
      for (j in seqq) {
        
        if (j == loc - 1)
          break
        
        val.antecedent.b <- MF[k, temp[j + 2]]
        ##condition for conjunction operator (AND)
        if ((temp[j + 1] == "1") || (temp[j + 1] == "and")){
          ##condition for type.tnorm used is standard type(min)
          if (type.tnorm == 1 || type.tnorm == "MIN"){
            if (!is.na(val.antecedent.b) && val.antecedent.b < val.antecedent){
              val.antecedent <- val.antecedent.b
              min.indx <- temp[j + 2]
            }
          }
          ##condition for type.tnorm used is Hamacher product
          else if (type.tnorm == 2 || type.tnorm == "HAMACHER") {									
            val.antecedent <- (val.antecedent * val.antecedent.b) / (val.antecedent + val.antecedent.b - val.antecedent * val.antecedent.b)
          }
          
          ##condition for type.tnorm used is yager class (with tao = 1)
          else if (type.tnorm == 3 || type.tnorm == "YAGER") {										
            temp.val.ante <- (1 - val.antecedent) + (1 - val.antecedent.b)
            if (temp.val.ante <= 1){
              val.antecedent <- temp.val.ante
            } else {
              val.antecedent <- 1
            }
          }
          
          ##condition for type.tnorm used is product
          else if (type.tnorm == 4 || type.tnorm == "PRODUCT") {				
            val.antecedent <- val.antecedent * val.antecedent.b
          }
          
          ##condition for type.tnorm used is bounded product
          else if (type.tnorm == 5 || type.tnorm == "BOUNDED"){
            temp.val.ante <- (val.antecedent * val.antecedent.b - 1)
            if (temp.val.ante > 0){
              val.antecedent <- temp.val.ante
            } else {
              val.antecedent <- 0
            }
          }
        }
        
        ##condition for disjunction operator (OR)
        else if ((temp[j + 1] == "2") || (temp[j + 1] == "or")){
          ##condition for type.snorm used is standard type (max)
          if (type.snorm == 1 || type.snorm == "MAX"){						
            if (val.antecedent.b > val.antecedent)
              val.antecedent <- val.antecedent.b
          }
          
          ##condition for type.snorm used is Hamacher sum
          else if (type.snorm == 2 || type.snorm == "HAMACHER") {							
            val.antecedent <- (val.antecedent + val.antecedent.b - 2 * val.antecedent * val.antecedent.b) / (1 - val.antecedent * val.antecedent.b)
          }
          
          ##condition for type.snorm used is yager class (with tao = 1)
          else if (type.snorm == 3 || type.snorm == "YAGER"){							
            temp.val.ante <- (val.antecedent + val.antecedent.b)
            if (temp.val.ante <= 1){
              val.antecedent <- temp.val.ante
            } else {
              val.antecedent <- 1						
            }
          }
          
          ##condition for type.snorm used is sum
          else if (type.snorm == 4 || type.snorm == "SUM"){
            val.antecedent <- (val.antecedent + val.antecedent.b - val.antecedent * val.antecedent.b)
          }
          
          ##condition for type.snorm used is bounded sum
          else if (type.snorm == 5 || type.snorm == "BOUNDED"){
            temp.val.ante <- (val.antecedent + val.antecedent.b)
            if (temp.val.ante <= 1){
              val.antecedent <- temp.val.ante
            } else {
              val.antecedent <- 1					
            }
          }				
        }
        
      }
      
      ##save value of MF on each rule			
      miu.rule[k, i] <- c(val.antecedent)		
      miu.rule.indx[k, i] <- min.indx
    }	
  }
  ## result 
  ## number of row is based on number of data.
  ## number of column is based on number of rule.
  return(list(miu.rule = miu.rule, miu.rule.indx = miu.rule.indx))
}

inference.parallel<-function(MF, rule, names.varinput, type.tnorm, type.snorm){
  require(foreach)
  require(doParallel)
  require(parallel)
  require(iterators)
  require(doSNOW)
  
  cl <- makePSOCKcluster(detectCores())
  registerDoSNOW(cl)
  
  ##calculate number of data
  nMF <- nrow(MF)
  
  ##calculate number of rule
  nrule <- nrow(rule)
  
  ##give names on each column for detecting which rule will be fired
  colnames(MF) <- c(names.varinput)

  ##allocate memory for membership function on antecedent
  # miu.rule <- matrix(nrow = nMF, ncol = nrule)
  
  ##Iteration for n data
  miu.rule <- foreach(k = icount(nMF), .combine = rbind) %dopar%
  {
    rvector <- matrix(nrow = 1,ncol = nrule)
    for(i in 1:nrule)
    {
      ##change list of rule into matrix
      temp <- rule[i,]
      
      ##detect location of "->" sign as separation between antecedent and consequence
      loc <- which(temp == "->")
      
      val.antecedent <- MF[k,temp[1]]
      
      ## iterate to calculate degree of MF until find "->" sign (equals to loc)
      seqq <- seq(from = 1, to = loc, by = 2)
      for (j in seqq) {
        
        if (j == loc - 1)
          break
        
        val.antecedent.b <- MF[k,temp[j + 2]]
        ##condition for conjunction operator (AND)
        if ((temp[j + 1] == "1") || (temp[j + 1] == "and")){
          ##condition for type.tnorm used is standard type(min)
          if (type.tnorm == 1 || type.tnorm == "MIN"){
            if (val.antecedent.b < val.antecedent)
              val.antecedent <- val.antecedent.b
          }
          ##condition for type.tnorm used is Hamacher product
          else if (type.tnorm == 2 || type.tnorm == "HAMACHER") {
            val.antecedent <- (val.antecedent * val.antecedent.b) / (val.antecedent + val.antecedent.b - val.antecedent * val.antecedent.b)
          }
          
          ##condition for type.tnorm used is yager class (with tao = 1)
          else if (type.tnorm == 3 || type.tnorm == "YAGER") {
            temp.val.ante <- (1 - val.antecedent) + (1 - val.antecedent.b)
            if (temp.val.ante <= 1){
              val.antecedent <- temp.val.ante
            } else {
              val.antecedent <- 1
            }
          }
          
          ##condition for type.tnorm used is product
          else if (type.tnorm == 4 || type.tnorm == "PRODUCT") {
            val.antecedent <- val.antecedent * val.antecedent.b
          }
          
          ##condition for type.tnorm used is bounded product
          else if (type.tnorm == 5 || type.tnorm == "BOUNDED"){
            temp.val.ante <- (val.antecedent * val.antecedent.b - 1)
            if (temp.val.ante > 0){
              val.antecedent <- temp.val.ante
            } else {
              val.antecedent <- 0
            }
          }
        }
        
        ##condition for disjunction operator (OR)
        else if ((temp[j + 1] == "2") || (temp[j + 1] == "or")){
          ##condition for type.snorm used is standard type (max)
          if (type.snorm == 1 || type.snorm == "MAX"){
            if (val.antecedent.b > val.antecedent)
              val.antecedent <- val.antecedent.b
          }
          
          ##condition for type.snorm used is Hamacher sum
          else if (type.snorm == 2 || type.snorm == "HAMACHER") {
            val.antecedent <- (val.antecedent + val.antecedent.b - 2 * val.antecedent * val.antecedent.b) / (1 - val.antecedent * val.antecedent.b)
          }
          
          ##condition for type.snorm used is yager class (with tao = 1)
          else if (type.snorm == 3 || type.snorm == "YAGER"){
            temp.val.ante <- (val.antecedent + val.antecedent.b)
            if (temp.val.ante <= 1){
              val.antecedent <- temp.val.ante
            } else {
              val.antecedent <- 1
            }
          }
          
          ##condition for type.snorm used is sum
          else if (type.snorm == 4 || type.snorm == "SUM"){
            val.antecedent <- (val.antecedent + val.antecedent.b - val.antecedent * val.antecedent.b)
          }
          
          ##condition for type.snorm used is bounded sum
          else if (type.snorm == 5 || type.snorm == "BOUNDED"){
            temp.val.ante <- (val.antecedent + val.antecedent.b)
            if (temp.val.ante <= 1){
              val.antecedent <- temp.val.ante
            } else {
              val.antecedent <- 1
            }
          }
        }
        
      }
      
      ##save value of MF on each rule			
      rvector[,i] <- val.antecedent
      
    }
    return(rvector)
  }
  
  stopCluster(cl)
  
  ## result 
  ## number of row is based on number of data.
  ## number of column is based on number of rule.
  return(miu.rule)
}

defuzzifier <- function(data, rule = NULL, degree.rule, range.output = NULL, names.varoutput = NULL, varout.mf = NULL, miu.rule, type.defuz = NULL, type.model = "TSK", func.tsk = NULL){
  
  ## Inisialitation
  def <- matrix(0, nrow=nrow(data), ncol = 1)
  def.temp <- matrix(0, nrow=nrow(data), ncol = 1)
  
  y4.matrix <- matrix(0, nrow = nrow(data), ncol = length(names.varoutput))
  
  degree.rule.square <- degree.rule ^ 2
  ## Mamdani type
  if (type.model == 1 || type.model == "MAMDANI"){
    ## copy rule
    rule.temp <- matrix(rule, nrow = nrow(rule), ncol = ncol(rule))
    
    ## check names.varoutput
    if (is.null(names.varoutput)){
      stop("Please define the names of the output variable.")
    }
    
    ## check parameters of membership functions on output variables
    if (is.null(varout.mf)){
      stop("Please define the parameters of membership functions on the output variable.")
    }
    
    for (k in 1 : nrow(data)){
      ##Check not zero on miu.rule
      chck <- which(miu.rule[k, ] != 0)
      
      l.chck <- length(chck) ## number of non-zero miu.rule
      cum <- matrix(0, nrow=nrow(data), ncol = 1) 
      div <- matrix(0, nrow=nrow(data), ncol = 1)
      
      ## initialize
      temp <- 0
      temp.d <- 0
      temp1 <- 0
      temp2 <- 0
      
      #if there are some no zero element on miu rule
      if (l.chck != 0) {
        indx <- matrix(nrow = l.chck, ncol = 2)
        temp.indx <- matrix(nrow = 1, ncol = 2)
        ## along number of not zero on miu.rule, check and save string related on names.varoutput and its value
        for (ii in 1 : l.chck){
          #get the names of output value on each rule
          
          strg <- c(rule.temp[chck[ii], ncol(rule.temp)])
          aaa <- which(strg == names.varoutput)
          
          if (length(aaa) != 0) {
            indx[ii, 1] <- aaa
            indx[ii, 2] <- miu.rule[k, chck[ii]] * degree.rule.square[chck[ii]]^2
          }
        }
        
        if (nrow(indx) > 1) {
          ## check duplication on indx, choose with has a max of degree
          indx <- indx[order(indx[,c(2)], decreasing = TRUE),]
          indx.nondup <- which(duplicated(indx[, 1]) == FALSE, arr.ind = TRUE)
          indx <- indx[indx.nondup, ,drop = FALSE] 
        }
        
        indx.new <- matrix(0,nrow = 1, ncol = length(names.varoutput))
        for(ii in 1:nrow(indx)){
          indx.new[indx[ii,1]] <- indx[ii,2]
        }
        ## indx -> y4
        y4.matrix[k,] <- indx.new
        
        #defuzzification procedure for  Centroid
        if (type.defuz == 1 || type.defuz == "WAM") {								
          for (i in 1 : nrow(indx)){										
            # calculate modified centroid
            # update for gaussian
            
            if (any(varout.mf[1, indx[i, 1]] == c(5, 6, 7))) {
              ## center point
              av.point <- varout.mf[2, indx[i, 1]]											
            }
            
            else {						
              av.point <- varout.mf[3, indx[i, 1]] 							
            }
            ## indx is fired miu.rule/rule
            temp1 <- indx[i, 2] * av.point 
            temp2 <- indx[i, 2] 
            
            temp <- temp + temp1
            temp.d <- temp.d + temp2
            
            cum[k, i] <- temp
            div[k, i] <- temp.d   					
          }
          
          if (sum(div[k, ]) == 0){
            def[k, 1] <- (min(range.output, na.rm=TRUE) + max(range.output, na.rm = TRUE))/2
          }
          else{
            def.temp[k, 1] <- sum(cum[k, ]) / sum(div[k, ])
            if (def.temp[k, 1] <= min(range.output, na.rm=TRUE)){
              #def[k, 1] <- (min(range.output, na.rm=TRUE) + max(range.output, na.rm = TRUE))/2
              def[k, 1] <- min(range.output, na.rm=TRUE)
            }
            else if (def.temp[k, 1] >= max(range.output, na.rm=TRUE)){
              #def[k, 1] <- (min(range.output, na.rm=TRUE) + max(range.output, na.rm = TRUE))/2
              def[k, 1] <- max(range.output, na.rm = TRUE)
            }
            else {
              def[k, 1] <- def.temp[k, 1]
            }
          }
        }
        
        ## procedure for type.defuz == 2 (fisrt of Maxima) and type.defuz == 3 (last of maxima)
        else if (any(type.defuz == c(2, 3)) || any(type.defuz == c("FIRST.MAX", "LAST.MAX"))){
          max.temp <- max(indx[, 2], na.rm = TRUE)
          max.indx <- which(indx[, 2] == max.temp)			
          
          aa <- varout.mf[2, indx[max.indx[1], 1]]
          bb <- varout.mf[3, indx[max.indx[1], 1]]
          cc <- varout.mf[4, indx[max.indx[1], 1]]
          dd <- varout.mf[5, indx[max.indx[1], 1]]
          
          # check shape of membership function
          if (varout.mf[1, indx[max.indx[1], 1]] == 1){
            seqq <- seq(from = varout.mf[2, indx[max.indx[1], 1]], to = varout.mf[4, indx[max.indx[1], 1]], by = (varout.mf[4, indx[max.indx[1], 1]] - varout.mf[2, indx[max.indx[1], 1]]) / 10)
            
            for (j in 1:length(seqq)){
              if (seqq[j] < aa){
                temp.miu <- 1
              }
              else if (seqq[j] >= aa & seqq[j] < bb){
                temp.miu <- (seqq[j] - aa) / (bb - aa)
              }
              else if (seqq[j] >= bb & seqq[j] < cc) {
                temp.miu <- (seqq[j] - cc) / (bb - cc)
              }
              else 
                temp.miu <- 1
              
              if (temp.miu >= indx[max.indx[1], 2]) {
                def[k, 1] <- seqq[j]
                if (type.defuz == 2 || type.defuz == "FIRST.MAX"){
                  break
                }
              }
            }								
          }
          
          else if (varout.mf[1, indx[max.indx[1], 1]] == 2){
            seqq <- seq(from = varout.mf[2, indx[max.indx[1], 1]], to = varout.mf[4, indx[max.indx[1], 1]], by = (varout.mf[4, indx[max.indx[1], 1]] - varout.mf[2, indx[max.indx[1], 1]]) / 10)
            
            for (j in 1:length(seqq)){
              if (seqq[j] < bb){
                temp.miu <- 1
              }
              else if (seqq[j] >= bb & seqq[j] < cc){
                temp.miu <- (seqq[j] - cc) / (bb - cc)
              }
              else if (seqq[j] > cc) {
                temp.miu <- 1
              }
              
              if (temp.miu >= indx[max.indx[1], 2]) {
                def[k, 1] <- seqq[j]
                if (type.defuz == 2 || type.defuz == "FIRST.MAX"){
                  break
                }
              }
            }										
          }
          
          else if (varout.mf[1, indx[max.indx[1], 1]] == 3){
            seqq <- seq(from = varout.mf[2, indx[max.indx[1], 1]], to = varout.mf[4, indx[max.indx[1], 1]], by = (varout.mf[4, indx[max.indx[1], 1]] - varout.mf[2, indx[max.indx[1], 1]]) / 10)
            for (j in 1:length(seqq)){
              if (seqq[j] < aa){
                temp.miu <- 0
              }
              else if (seqq[j] >= aa & seqq[j] < bb){
                temp.miu <- (seqq[j] - aa) / (bb - aa)
              }
              else if (seqq[j] > cc) {
                temp.miu <- 1
              }
              
              if (temp.miu >= indx[max.indx[1], 2]) {
                def[k, 1] <- seqq[j]
                if (type.defuz == 2 || type.defuz == "FIRST.MAX"){
                  break
                }
              }
            }
          }
          
          else if (varout.mf[1, indx[max.indx[1], 1]] == 4) {
            seqq <- seq(from = varout.mf[2, indx[max.indx[1], 1]], to = varout.mf[4, indx[max.indx[1], 1]], by = (varout.mf[4, indx[max.indx[1], 1]] - varout.mf[2, indx[max.indx[1], 1]]) / 10)
            
            for (j in 1:length(seqq)){
              if (seqq[j] < aa){
                temp.miu <- 0
              }
              else if (seqq[j] >= aa & seqq[j] < bb){
                temp.miu <- (seqq[j] - aa) / (bb - aa)
              }
              else if (seqq[j] >= bb & seqq[j] < cc) {
                temp.miu <- 1
              }
              else if (seqq[j] >= cc & seqq[j] < dd) {
                temp.miu <- (seqq[j] - dd) / (cc - dd)
              }
              else {
                temp.miu <- 0
              }
              
              if (temp.miu >= indx[max.indx[1], 2]) {
                def[k, 1] <- seqq[j]
                if (type.defuz == 2 || type.defuz == "FIRST.MAX"){
                  break
                }
              }
            }
          }
          
          else if (varout.mf[1, indx[max.indx[1], 1]] == 5) {
            seqq <- seq(from = min(range.output) , to = max(range.output), by = (max(range.output) - min(range.output)) / 100)
            
            for (j in 1:length(seqq)){
              
              temp.miu <- exp(- 0.5 * (seqq[j] - aa) ^ 2 / bb ^ 2)
              
              if (temp.miu >= indx[max.indx[1], 2]) {
                def[k, 1] <- seqq[j]
                if (type.defuz == 2 || type.defuz == "FIRST.MAX"){
                  break
                }
              }
              else {
                def[k, 1] <- (min(range.output, na.rm=TRUE) + max(range.output, na.rm = TRUE))/2
              }
            }
          }
          
          else if (varout.mf[1, indx[max.indx[1], 1]] == 6) {
            seqq <- seq(from = min(range.output) , to = max(range.output), by = (max(range.output) - min(range.output)) / 10)
            for (j in 1:length(seqq)){
              
              temp.miu <- 1 / (1 + exp(- aa * (seqq[j] - bb)))
              
              if (temp.miu >= indx[max.indx[1], 2]) {
                def[k, 1] <- seqq[j]
                if (type.defuz == 2 || type.defuz == "FIRST.MAX"){
                  break
                }
              }
              else {
                def[k, 1] <- (min(range.output, na.rm=TRUE) + max(range.output, na.rm = TRUE))/2
              }
            }
          }
          
          else if (varout.mf[1, indx[max.indx[1], 1]] == 7) {
            seqq <- seq(from = min(range.output) , to = max(range.output), by = (max(range.output) - min(range.output)) / 10)
            for (j in 1:length(seqq)){
              
              temp.miu <- 1 / (1 + abs((seqq[j] - cc)/aa) ^ (2 * bb))
              
              if (temp.miu >= indx[max.indx[1], 2]) {
                def[k, 1] <- seqq[j]
                if (type.defuz == 2 || type.defuz == "FIRST.MAX"){
                  break
                }
              }
              else {
                def[k, 1] <- (min(range.output, na.rm=TRUE) + max(range.output, na.rm = TRUE))/2
              }
            }
          }
        }	
        
        ## procedure for type.defuz == 4 (mean of maxima)
        else if (type.defuz == 4 || type.defuz == "MEAN.MAX") {
          max.temp <- max(indx[, 2], na.rm = TRUE)
          max.indx <- which(indx[, 2] == max.temp)			
          
          def[k, 1] <- 0.5 * (max(varout.mf[2:5, indx[max.indx[1], 1]], na.rm = TRUE) + (varout.mf[2, indx[max.indx[1], 1]]))
        }
        
        else if (type.defuz == 5 || type.defuz == "COG") {
          temp <- 0
          temp.d <- 0
          for (i in 1 : nrow(indx)){
            #calculate modified centroid
            # update for gaussian
            if (any(varout.mf[1, indx[i, 1]] == c(5, 6,7))) {
              ## Gaussian Distribution
              ## center point
              mean.point <- varout.mf[2, indx[i, 1]]
              
              ## indx is fired miu.rule/rule
              temp1 <- indx[i, 2] * mean.point * varout.mf[3, indx[i, 1]]
              temp2 <- indx[i, 2] * varout.mf[3, indx[i, 1]]
              
              temp <- temp + temp1
              temp.d <- temp.d + temp2
            }else{				
              mean.point <- varout.mf[3, indx[i, 1]]
              # check first which one greater between indx[i, 2] with the value of MF (for centroid)
              temp1 <- indx[i, 2] * mean.point 
              temp2 <- indx[i, 2] 
              temp <- temp + temp1
              temp.d <- temp.d + temp2
            }   					
          }
          cum[k, ] <- temp
          div[k, ] <- temp.d
          
          if (sum(div[k, ]) == 0){
            def[k, 1] <- (min(range.output, na.rm=TRUE) + max(range.output, na.rm = TRUE))/2
          }else{
            def.temp[k, 1] <- sum(cum[k, ]) / sum(div[k, ])
            if (def.temp[k, 1] <= min(range.output, na.rm=TRUE)){
              #def[k, 1] <- (min(range.output, na.rm=TRUE) + max(range.output, na.rm = TRUE))/2
              def[k, 1] <- min(range.output, na.rm=TRUE)
            }
            else if (def.temp[k, 1] >= max(range.output, na.rm=TRUE)){
              #def[k, 1] <- (min(range.output, na.rm=TRUE) + max(range.output, na.rm = TRUE))/2
              def[k, 1] <- max(range.output, na.rm = TRUE)
            }
            else {
              def[k, 1] <- def.temp[k, 1]
            }
          }
        }
        
      }
      else {
        def[k, 1] <- (min(range.output, na.rm=TRUE) + max(range.output, na.rm = TRUE))/2
      }
    }	
    
  }
  
  
  #TSK type
  else if (type.model == 2 || type.model == "TSK"){
    
    ## check the linear equation on consequent part
    if (is.null(func.tsk)){
      stop("Please define the linear equations on the consequent parts of the fuzzy IF-THEN rules.")
    }
    
    for (k in 1 : nrow(data)){
      data.m <- data[k, ,drop = FALSE]
      
      if (ncol(func.tsk) > 1){
        func.tsk.var <- func.tsk[, -ncol(func.tsk), drop = FALSE]
        func.tsk.cont <- func.tsk[, ncol(func.tsk), drop = FALSE]
        
        ff <- func.tsk.var %*% t(data.m) + func.tsk.cont
      }
      else if (ncol(func.tsk) == 1){
        ff <- func.tsk 
      }
      
      miu.rule.t <- miu.rule[k, ,drop = FALSE]
      
      cum <- miu.rule.t %*% ff
      div <- sum(miu.rule.t)
      
      def[k, 1] <- cum / div
      
      if (div == 0)
        def[k, 1] <- 0
      else
      {
        def[k, 1] <- cum / div
        if (def[k, 1] > max(range.output))
          def[k, 1] <- max(range.output)
        else if (def[k, 1] < min(range.output))
          def[k, 1] <- min(range.output)
      }
    }
  }
  
  return(list(y5 = def, y4 = y4.matrix))
}

defuzzifier.parallel <- function(data, rule = NULL, degree.rule, range.output = NULL, names.varoutput = NULL, varout.mf = NULL, miu.rule, type.defuz = NULL, type.model = "TSK", func.tsk = NULL){
  
  require(foreach)
  require(doParallel)
  require(doSNOW)
  require(parallel)
  
  cl <- makePSOCKcluster(detectCores())
  registerDoSNOW(cl)
  
  degree.rule.square <- degree.rule ^ 2
  
  ## Mamdani type
  if (type.model == 1 || type.model == "MAMDANI"){
    ## copy rule
    rule.temp <- matrix(rule, nrow = nrow(rule), ncol = ncol(rule))
    
    ## check names.varoutput
    if (is.null(names.varoutput)){
      stop("Please define the names of the output variable.")
    }
    
    ## check parameters of membership functions on output variables
    if (is.null(varout.mf)){
      stop("Please define the parameters of membership functions on the output variable.")
    }
    
    ## Inisialitation
    cum <- matrix(0, nrow=nrow(data), ncol = ncol(varout.mf)) 
    div <- matrix(0, nrow=nrow(data), ncol = ncol(varout.mf))
    
    # y4.matrix <- matrix(0, ncol = length(names.varoutput), nrow = nrow(data))
    
    def <- foreach (k = 1 : nrow(data), .combine = rbind) %dopar%
    {
      ##Check not zero on miu.rule
      chck <- which(miu.rule[k, ] != 0)
      
      l.chck <- length(chck)
      cum <- matrix(0, nrow=nrow(data), ncol = 1) 
      div <- matrix(0, nrow=nrow(data), ncol = 1)
      
      ## initialize
      temp1 <- 0
      temp2 <- 0
      
      #if there are some no zero element on miu rule
      if (l.chck != 0) {
        indx <- matrix(nrow = l.chck, ncol = 2)
        temp.indx <- matrix(nrow = 1, ncol = 2)
        ## along number of not zero on miu.rule, check and save string related on names.varoutput and its value
        for (ii in 1 : l.chck){
          #get the names of output value on each rule
          
          strg <- c(rule.temp[chck[ii], ncol(rule.temp)])
          aaa <- which(strg == names.varoutput)
          
          if (length(aaa) != 0) {
            indx[ii, 1] <- aaa
            indx[ii, 2] <- miu.rule[k, chck[ii]] #* degree.rule.square[chck[ii]]
          }
        }
        
        if (nrow(indx) > 1) {
          ## check duplication on indx, choose with has a max of degree
          indx <- indx[order(indx[,c(2)], decreasing = TRUE),]
          indx.nondup <- which(duplicated(indx[, 1]) == FALSE, arr.ind = TRUE)
          indx <- indx[indx.nondup, ,drop = FALSE] 
        }
        
        #defuzzification procedure for  Centroid
        if (type.defuz == 1 || type.defuz == "WAM") {								
          for (i in 1 : nrow(indx)){										
            # calculate modified centroid
            # update for gaussian
            
            if (any(varout.mf[1, indx[i, 1]] == c(5, 6, 7))) {
              ## center point
              av.point <- varout.mf[2, indx[i, 1]]											
            }
            
            else {						
              av.point <- varout.mf[3, indx[i, 1]] 							
            }
            ## indx is fired miu.rule/rule
            temp1 <- indx[i, 2] * av.point 
            temp2 <- indx[i, 2] 
            
            temp <- temp + temp1
            temp.d <- temp.d + temp2
            
            cum[k, i] <- temp
            div[k, i] <- temp.d   					
          }
          
          if (sum(div[k, ]) == 0){
            return((min(range.output, na.rm=TRUE) + max(range.output, na.rm = TRUE))/2)
          }
          else{
            def.temp <- sum(cum[k, ]) / sum(div[k, ])
            if (def.temp <= min(range.output, na.rm=TRUE)){
              #def[k, 1] <- (min(range.output, na.rm=TRUE) + max(range.output, na.rm = TRUE))/2
              return(min(range.output, na.rm=TRUE))
            }
            else if (def.temp >= max(range.output, na.rm=TRUE)){
              #def[k, 1] <- (min(range.output, na.rm=TRUE) + max(range.output, na.rm = TRUE))/2
              return(max(range.output, na.rm = TRUE))
            }
            else {
              return(def.temp)
            }
          }
        }
        
        ## procedure for type.defuz == 2 (fisrt of Maxima) and type.defuz == 3 (last of maxima)
        else if (any(type.defuz == c(2, 3)) || any(type.defuz == c("FIRST.MAX", "LAST.MAX"))){
          max.temp <- max(indx[, 2], na.rm = TRUE)
          max.indx <- which(indx[, 2] == max.temp)

          aa <- varout.mf[2, indx[max.indx[1], 1]]
          bb <- varout.mf[3, indx[max.indx[1], 1]]
          cc <- varout.mf[4, indx[max.indx[1], 1]]
          dd <- varout.mf[5, indx[max.indx[1], 1]]

          # check shape of membership function
          if (varout.mf[1, indx[max.indx[1], 1]] == 1){
            seqq <- seq(from = varout.mf[2, indx[max.indx[1], 1]], to = varout.mf[4, indx[max.indx[1], 1]], by = (varout.mf[4, indx[max.indx[1], 1]] - varout.mf[2, indx[max.indx[1], 1]]) / 10)

            for (j in 1:length(seqq)){
              if (seqq[j] < aa){
                temp.miu <- 1
              }
              else if (seqq[j] >= aa & seqq[j] < bb){
                temp.miu <- (seqq[j] - aa) / (bb - aa)
              }
              else if (seqq[j] >= bb & seqq[j] < cc) {
                temp.miu <- (seqq[j] - cc) / (bb - cc)
              }
              else
                temp.miu <- 1

              if (temp.miu >= indx[max.indx[1], 2]) {
                def[k, 1] <- seqq[j]
                if (type.defuz == 2 || type.defuz == "FIRST.MAX"){
                  break
                }
              }
            }
          }

          else if (varout.mf[1, indx[max.indx[1], 1]] == 2){
            seqq <- seq(from = varout.mf[2, indx[max.indx[1], 1]], to = varout.mf[4, indx[max.indx[1], 1]], by = (varout.mf[4, indx[max.indx[1], 1]] - varout.mf[2, indx[max.indx[1], 1]]) / 10)

            for (j in 1:length(seqq)){
              if (seqq[j] < bb){
                temp.miu <- 1
              }
              else if (seqq[j] >= bb & seqq[j] < cc){
                temp.miu <- (seqq[j] - cc) / (bb - cc)
              }
              else if (seqq[j] > cc) {
                temp.miu <- 1
              }

              if (temp.miu >= indx[max.indx[1], 2]) {
                def[k, 1] <- seqq[j]
                if (type.defuz == 2 || type.defuz == "FIRST.MAX"){
                  break
                }
              }
            }
          }
          else if (varout.mf[1, indx[max.indx[1], 1]] == 3){
            seqq <- seq(from = varout.mf[2, indx[max.indx[1], 1]], to = varout.mf[4, indx[max.indx[1], 1]], by = (varout.mf[4, indx[max.indx[1], 1]] - varout.mf[2, indx[max.indx[1], 1]]) / 10)
            for (j in 1:length(seqq)){
              if (seqq[j] < aa){
                temp.miu <- 0
              }
              else if (seqq[j] >= aa & seqq[j] < bb){
                temp.miu <- (seqq[j] - aa) / (bb - aa)
              }
              else if (seqq[j] > cc) {
                temp.miu <- 1
              }

              if (temp.miu >= indx[max.indx[1], 2]) {
                def[k, 1] <- seqq[j]
                if (type.defuz == 2 || type.defuz == "FIRST.MAX"){
                  break
                }
              }
            }
          }
          else if (varout.mf[1, indx[max.indx[1], 1]] == 4) {
            seqq <- seq(from = varout.mf[2, indx[max.indx[1], 1]], to = varout.mf[4, indx[max.indx[1], 1]], by = (varout.mf[4, indx[max.indx[1], 1]] - varout.mf[2, indx[max.indx[1], 1]]) / 10)

            for (j in 1:length(seqq)){
              if (seqq[j] < aa){
                temp.miu <- 0
              }
              else if (seqq[j] >= aa & seqq[j] < bb){
                temp.miu <- (seqq[j] - aa) / (bb - aa)
              }
              else if (seqq[j] >= bb & seqq[j] < cc) {
                temp.miu <- 1
              }
              else if (seqq[j] >= cc & seqq[j] < dd) {
                temp.miu <- (seqq[j] - dd) / (cc - dd)
              }
              else {
                temp.miu <- 0
              }

              if (temp.miu >= indx[max.indx[1], 2]) {
                def[k, 1] <- seqq[j]
                if (type.defuz == 2 || type.defuz == "FIRST.MAX"){
                  break
                }
              }
            }
          }

          else if (varout.mf[1, indx[max.indx[1], 1]] == 5) {
            seqq <- seq(from = min(range.output) , to = max(range.output), by = (max(range.output) - min(range.output)) / 100)

            for (j in 1:length(seqq)){

              temp.miu <- exp(- 0.5 * (seqq[j] - aa) ^ 2 / bb ^ 2)

              if (temp.miu >= indx[max.indx[1], 2]) {
                def[k, 1] <- seqq[j]
                if (type.defuz == 2 || type.defuz == "FIRST.MAX"){
                  break
                }
              }
              else {
                def[k, 1] <- (min(range.output, na.rm=TRUE) + max(range.output, na.rm = TRUE))/2
              }
            }
          }

          else if (varout.mf[1, indx[max.indx[1], 1]] == 6) {
            seqq <- seq(from = min(range.output) , to = max(range.output), by = (max(range.output) - min(range.output)) / 10)
            for (j in 1:length(seqq)){

              temp.miu <- 1 / (1 + exp(- aa * (seqq[j] - bb)))

              if (temp.miu >= indx[max.indx[1], 2]) {
                def[k, 1] <- seqq[j]
                if (type.defuz == 2 || type.defuz == "FIRST.MAX"){
                  break
                }
              }
              else {
                def[k, 1] <- (min(range.output, na.rm=TRUE) + max(range.output, na.rm = TRUE))/2
              }
            }
          }
          else if (varout.mf[1, indx[max.indx[1], 1]] == 7) {
            seqq <- seq(from = min(range.output) , to = max(range.output), by = (max(range.output) - min(range.output)) / 10)
            for (j in 1:length(seqq)){

              temp.miu <- 1 / (1 + abs((seqq[j] - cc)/aa) ^ (2 * bb))

              if (temp.miu >= indx[max.indx[1], 2]) {
                def[k, 1] <- seqq[j]
                if (type.defuz == 2 || type.defuz == "FIRST.MAX"){
                  break
                }
              }
              else {
                def[k, 1] <- (min(range.output, na.rm=TRUE) + max(range.output, na.rm = TRUE))/2
              }
            }
          }
        }

        ## procedure for type.defuz == 4 (mean of maxima)
        else if (type.defuz == 4 || type.defuz == "MEAN.MAX") {
          max.temp <- max(indx[, 2], na.rm = TRUE)
          max.indx <- which(indx[, 2] == max.temp)

          def[k, 1] <- 0.5 * (max(varout.mf[2:5, indx[max.indx[1], 1]], na.rm = TRUE) + (varout.mf[2, indx[max.indx[1], 1]]))
        }

        else if (type.defuz == 5 || type.defuz == "COG") {
          temp <- 0
          temp.d <- 0
          for (i in 1 : nrow(indx)){
            #calculate modified centroid
            # update for gaussian
            if (any(varout.mf[1, indx[i, 1]] == c(5, 6,7))) {
              ## center point

              av.point <- varout.mf[2, indx[i, 1]]

              ## indx is fired miu.rule/rule
              temp1 <- indx[i, 2] * av.point * varout.mf[3, indx[i, 1]]
              temp2 <- indx[i, 2] * varout.mf[3, indx[i, 1]]

              temp <- temp + temp1
              temp.d <- temp.d + temp2
            }

            else {
              av.point <- varout.mf[3, indx[i, 1]]
              # check first which one greater between indx[i, 2] with the value of MF (for centroid)
              temp1 <- indx[i, 2] * av.point
              temp2 <- indx[i, 2]
              temp <- temp + temp1
              temp.d <- temp.d + temp2
            }
          }
          cum[k, ] <- temp
          div[k, ] <- temp.d

          if (sum(div[k, ]) == 0){
            return((min(range.output, na.rm=TRUE) + max(range.output, na.rm = TRUE))/2)
          }
          else{
            def.temp <- sum(cum[k, ]) / sum(div[k, ])
            if (def.temp <= min(range.output, na.rm=TRUE)){
              #def[k, 1] <- (min(range.output, na.rm=TRUE) + max(range.output, na.rm = TRUE))/2
              return(min(range.output, na.rm=TRUE))
            }
            else if (def.temp >= max(range.output, na.rm=TRUE)){
              #def[k, 1] <- (min(range.output, na.rm=TRUE) + max(range.output, na.rm = TRUE))/2
              return(max(range.output, na.rm = TRUE))
            }
            else {
              return(def.temp)
            }
          }
        }
        
      }
      else {
        def.temp <- (min(range.output, na.rm=TRUE) + max(range.output, na.rm = TRUE))/2
        return(def.temp)
      }
      
    }	
    
  }
  
  stopCluster(cl)
    
  return(list(y5 = def, y4 = y4.matrix))
}

ch.unique.fuzz <- function(type.model, rule, varinp.mf, varout.mf = NULL, num.varinput, num.labels){	
  num.labels.input <- num.labels[, -ncol(num.labels), drop = FALSE]	
  
  ## Change the linguistic values on input data
  seq.names <- rep(1:num.varinput, num.labels.input)
  names.fvalinput <- paste(colnames(varinp.mf), seq.names, sep=".")
  colnames(varinp.mf) <- names.fvalinput
  ## Change the linguistic values on output data
  if (type.model == "MAMDANI"){
    seq.names <- rep(num.varinput + 1, num.labels[, ncol(num.labels)])
    names.fvaloutput <- paste(colnames(varout.mf), seq.names, sep=".")
    colnames(varout.mf) <- names.fvaloutput
    names.fvalues <- c(names.fvalinput, names.fvaloutput)
  }
  else if (type.model == "TSK"){
    names.fvaloutput <- colnames(varout.mf)
    names.fvalues <- c(names.fvalinput, names.fvaloutput)
  }
  
  return (list(rule = rule, varinp.mf = varinp.mf, varout.mf = varout.mf, 
               names.fvalinput = names.fvalinput, names.fvaloutput = names.fvaloutput, 
               names.fvalues = names.fvalues))
}
