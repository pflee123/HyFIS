denorm.data <- function(data.norm, range.data, min.scale = 0, max.scale = 1){
  if(!is.matrix(data.norm)){
    data.norm <- as.matrix(data.norm, ncol = 1)
  }
  row.data <- nrow(data.norm)
  col.data <- ncol(data.norm)
  data.denorm <- matrix(nrow = row.data, ncol=col.data)
  
  range.data <- as.matrix(range.data)
  # denormalize all data on each column 
  for (j in 1:col.data){
    min.data <- range.data[1, j]
    max.data <- range.data[2, j]
    data.denorm[, j] <- min.data + ((data.norm[, j] - min.scale)*(max.data - min.data))/ (max.scale - min.scale)
  }
  
  colnames(data.denorm) <- colnames(data.norm)
  return(data.denorm)
}

norm.data <- function(data.original, range.data, min.scale = 0, max.scale = 1){
  row.data <- nrow(data.original)
  col.data <- ncol(data.original)
  data.norm <- matrix(nrow = row.data, ncol=col.data)
  
  # normalize all data on each column 
  for (j in 1:col.data){
    min.data <- range.data[1, j]
    max.data <- range.data[2, j]
    
    data.norm[, j] <- min.scale + (data.original[, j] - min.data) * (max.scale - min.scale) / (max.data - min.data)
  }
  
  colnames(data.norm) <- colnames(data.original)
  return(data.norm)
}