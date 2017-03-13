library(frbs)

data(frbsData)

data.train <- frbsData$GasFurnance.dt

colnames(data.train) <- c("V1","V2","Output")

range <- apply(data.train, 2, range)

num.labels <- c(5,7,5)

object <- Train(data.train, model.type = "WM", range.data = range, num.labels = num.labels)

result <- Prediction(object, data.train[,-ncol(data.train)])
