WM <- function(data.train, num.labels, range.data.ori, type.mf = "GAUSSIAN", type.tnorm = "PRODUCT", type.snorm = "SUM", type.defuzz = "COG") {

  range.data <- matrix(nrow = 2, ncol = ncol(data.train))
  range.data[1, ] <- 0
  range.data[2, ] <- 1
  
  alpha <- 0.35
  # get number of row and column
  nrow.data.train <- nrow(data.train)
  num.varinput <- ncol(data.train)
  input_mf <- matrix(0, nrow = 5, ncol = sum(num.labels))
  
  ####### Wang and Mendel's Steps begin #######
  ## Step 1: Divide the Input and Output Spaces Into Fuzzy Regions####
  
  ## initialize
  index_cum <- cumsum(c(0,num.labels))
  
  ## loop for all variable
  for (i in 1 : num.varinput){
    ## initialize
    num_labels <- num.labels[i]
    
    ## Make the depth on each linguistic value, assumed it's similar on all region. 
    interval <- 1 / (num_labels - 1)
    mean_points <- 0:(num_labels-1) * interval
    
    ##contruct matrix of parameter of membership function (var.mf) on each variable (max(var) is num.varinput
   
    ##Type 1: Gaussian
    ##parameter=(mean a, standard deviation b)
    ##a=var.mf[2,]
    ##b=var.mf[3,]
    if (type.mf == 1 || type.mf == "GAUSSIAN") {
      input_mf[1, index_cum[i] + 1:num_labels] <- 1
      input_mf[2, index_cum[i] + 1:num_labels] <- mean_points
      input_mf[3, index_cum[i] + 1:num_labels] <- interval * alpha
    }
  }
  
  label_names <- unlist(sapply(1:ncol(data.train), function(x){
    paste(colnames(data.train)[x], 1:num.labels[x], sep = "_")
  }))
  
  colnames(input_mf) <- label_names
  ## Step 2: Generate Fuzzy Rules from Given Data Pairs. ####
  ## Step 2a: Determine the degree of data pairs.
  ## MF is matrix membership function. The dimension of MF is n x m, where n is number of data and m is num.labels * input variable (==ncol(var.mf))
  
  ## get degree of membership by fuzzification
  MF <- fuzzifier(data.train, num.varinput, num.labels, input_mf)
  
  colnames(MF) <- label_names
  
  ####get max value of degree on each variable to get one rule.
  MF.max <- matrix(0, nrow = nrow(MF), ncol = ncol(MF))
  
  for (i in 1 : length(num.labels)){
    ind <- index_cum[i] + 1:num.labels[i]
    MF_temp <- MF[, ind]
    
    for (m in 1 : nrow(MF)){
      max_MF_Temp <- max(MF_temp[m, ])
      max_loc <- index_cum[i] + which.max(MF_temp[m, ])
      MF.max[m, max_loc] <- max_MF_Temp	
    }
  }
  
  colnames(MF.max) <- label_names
  
  ## Step 3 ####
  ## determine the degree of the rule
  rule.matrix <- MF.max
  
  degree_rule <- matrix(nrow = nrow(rule.matrix), ncol =1)
  
  if(type.tnorm == "PRODUCT")
    degree_rule <- apply(rule.matrix, 1, function(x){
      prod(x[x != 0])
    })
  
  rule.matrix[rule.matrix > 0] <- 1
  rule.matrix.bool <- rule.matrix
  
  temp <- cbind(degree_rule, rule.matrix.bool)
  # order by degree.rule
  temp <- temp[order(temp[,1], decreasing = TRUE),]
  
  output.labels.indx <- index_cum[num.varinput] + 1:num.labels[num.varinput]
  ## find the same elements on matrix rule considering degree of rules
  indx.nondup <- !duplicated(temp[,-c(1, 1+output.labels.indx)])
  rule.complete <- temp[indx.nondup, ,drop = FALSE]
  degree_rule <- rule.complete[, 1, drop = FALSE]
  rule <- rule.complete[, -1, drop = FALSE]
  
  ## delete incomplete rule
  incomplete <- which(rowSums(rule) != num.varinput)
  rule[incomplete, ] <- NA	
  degree_rule[incomplete] <- NA
  
  rule <- na.omit(rule)
  degree_rule <- na.omit(degree_rule)
  
  ## create rule in numeric
  seqq <- seq(1:ncol(rule))
  rule.num <- t(apply(rule, 1, function(x) x * seqq))
  
  ## the number of labels
  ## rule
  ## membership functions
  ## the number of input variables
  ## the number of output varibales
  ## range of original dataset
  mod <- structure(list(model = "WM", num.labels = num.labels, rule = rule, rule.num = rule.num, 
                        membership_function = input_mf, mf_label_name = colnames(input_mf),
                        input_num = num.varinput-1, input_name = colnames(data.train[,-num.varinput]),
                        output_num = 1, output_name = colnames(data.train[, num.varinput]),
                        range.data.ori = range.data.ori,
                        degree.rule = degree_rule, type.mf = type.mf, type.tnorm = type.tnorm, type.snorm = type.snorm, type.defuzz = type.defuzz), class = "RuleBasedModel")
  
  return (mod)
}