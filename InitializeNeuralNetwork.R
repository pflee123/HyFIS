InitializeNeuralNetwork <- function (data, model.list, hidden, act.fct, err.fct, algorithm, linear.output, formula) 
  {
    formula.reverse <- formula
    formula.reverse[[2]] <- stats::as.formula(paste(model.list$response[[1]], 
                                                    "~", model.list$variables[[1]], sep = ""))[[2]]
    formula.reverse[[3]] <- formula[[2]]
    response <- as.matrix(stats::model.frame(formula.reverse, data))
    formula.reverse[[3]] <- formula[[3]]
    covariate <- as.matrix(stats::model.frame(formula.reverse, data))
    covariate[, 1] <- 1
    colnames(covariate)[1] <- "intercept"
    if (is.function(act.fct)) {
      act.deriv.fct <- differentiate(act.fct)
      attr(act.fct, "type") <- "function"
    }
    else {
      if (act.fct == "tanh") {
        act.fct <- function(x) {
          tanh(x)
        }
        attr(act.fct, "type") <- "tanh"
        act.deriv.fct <- function(x) {
          1 - x^2
        }
      }
      else if (act.fct == "logistic") {
        act.fct <- function(x) {
          1/(1 + exp(-x))
        }
        attr(act.fct, "type") <- "logistic"
        act.deriv.fct <- function(x) {
          x * (1 - x)
        }
      }
    }
    
    if (is.function(err.fct)) {
      err.deriv.fct <- differentiate(err.fct)
      attr(err.fct, "type") <- "function"
    }
    else {
      if (err.fct == "ce") {
        if (all(response == 0 | response == 1)) {
          err.fct <- function(x, y) {
            -(y * log(x) + (1 - y) * log(1 - x))
          }
          attr(err.fct, "type") <- "ce"
          err.deriv.fct <- function(x, y) {
            (1 - y)/(1 - x) - y/x
          }
        }
        else {
          err.fct <- function(x, y) {
            1/2 * (y - x)^2
          }
          attr(err.fct, "type") <- "sse"
          err.deriv.fct <- function(x, y) {
            x - y
          }
          warning("'err.fct' was automatically set to sum of squared error (sse), because the response is not binary", 
                  call. = F)
        }
      }
      else if (err.fct == "sse") {
        err.fct <- function(x, y) {
          1/2 * (y - x)^2
        }
        attr(err.fct, "type") <- "sse"
        err.deriv.fct <- function(x, y) {
          x - y
        }
      }
    }
    return(list(covariate = covariate, response = response, err.fct = err.fct, 
                err.deriv.fct = err.deriv.fct, act.fct = act.fct, act.deriv.fct = act.deriv.fct))
  }