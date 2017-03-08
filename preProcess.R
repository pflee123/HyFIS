denorm.data <- function(dt.norm, range.data, min.scale = 0, max.scale = 1){
  row.data <- nrow(dt.norm)
  col.data <- ncol(dt.norm)
  data.denorm <- matrix(nrow = row.data, ncol=col.data)
  
  range.data <- as.matrix(range.data)
  # denormalize all data on each column 
  for (j in 1:col.data){
    min.data <- range.data[1, j]
    max.data <- range.data[2, j]
    data.denorm[, j] <- min.data + ((dt.norm[, j] - min.scale)*(max.data - min.data))/ (max.scale - min.scale)
  }
  
  return(data.denorm)
}

norm.data <- function(dt.ori, range.data, min.scale = 0, max.scale = 1){
  row.data <- nrow(dt.ori)
  col.data <- ncol(dt.ori)
  data.norm <- matrix(nrow = row.data, ncol=col.data)
  
  # normalize all data on each column 
  for (j in 1:col.data){
    min.data <- range.data[1, j]
    max.data <- range.data[2, j]
    
    data.norm[, j] <- min.scale + (dt.ori[, j] - min.data) * (max.scale - min.scale) / (max.data - min.data)
  }
  
  return(data.norm)
}