library(frbs)

data(frbsData)

data.train <- frbsData$GasFurnance.dt

range <- apply(data.train, 2, range)

data.norm <- norm.data(data.train, range)

num.labels <- c(3,3,3)

object <- WM(data.norm, num.labels, range)

data.result <- denorm.data(data.norm, range.data = range)