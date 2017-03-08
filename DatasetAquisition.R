DatasetAquisition.experiment <- function(conn, start.train = (Sys.Date() - 
    15), end.train = Sys.Date(), start.test = (Sys.Date() - 15), 
    end.test = Sys.Date()) {
  
        require(tidyr, quietly = TRUE)
        require(dplyr, quietly = TRUE)
        start <- min(c(start.train, start.test, end.train, end.test))
        end <- max(c(start.train, start.test, end.train, end.test))
        sqlquery <- paste("SELECT [Trade_Date],[delivery_date],[delivery_hour],[delivery_interval],[SMP] FROM [dbo].[WA_SEMO_MarketResults] where [delivery_date] between '", 
            (start - 20), "' and '", (end + 1), "' order by delivery_date asc, delivery_hour asc, delivery_interval asc")
        dataset.smp <- sqlQuery(conn, sqlquery)
        dataset.smp[dataset.smp < 0] <- 0
        dataset.smp$delivery_date <- as.Date(as.character(dataset.smp$delivery_date))
        dataset.smp$Trade_Date <- as.Date(as.character(dataset.smp$Trade_Date))
        
        dataset.smp.lag1 <- dataset.smp[, c("Trade_Date", "delivery_date", 
            "delivery_hour", "delivery_interval")]
        dataset.smp.lag1$delivery_date <- dataset.smp$delivery_date + 
            1
        dataset.smp.lag1$Trade_Date <- dataset.smp$Trade_Date + 
            1
        dataset.smp.lag1$SMP_D_Minus_1_Euro <- dataset.smp$SMP
        
        dataset.smp.lag2 <- dataset.smp[, c("Trade_Date", "delivery_date", 
            "delivery_hour", "delivery_interval")]
        dataset.smp.lag2$delivery_date <- dataset.smp$delivery_date + 
            2
        dataset.smp.lag2$Trade_Date <- dataset.smp$Trade_Date + 
            2
        dataset.smp.lag2$SMP_D_Minus_2_Euro <- dataset.smp$SMP
        
        dataset.smp.lag3 <- dataset.smp[, c("Trade_Date", "delivery_date", 
            "delivery_hour", "delivery_interval")]
        dataset.smp.lag3$delivery_date <- dataset.smp$delivery_date + 
            3
        dataset.smp.lag3$Trade_Date <- dataset.smp$Trade_Date + 
            3
        dataset.smp.lag3$SMP_D_Minus_3_Euro <- dataset.smp$SMP
        
        dataset.smp.lag4 <- dataset.smp[, c("Trade_Date", "delivery_date", 
            "delivery_hour", "delivery_interval")]
        dataset.smp.lag4$delivery_date <- dataset.smp$delivery_date + 
            4
        dataset.smp.lag4$Trade_Date <- dataset.smp$Trade_Date + 
            4
        dataset.smp.lag4$SMP_D_Minus_4_Euro <- dataset.smp$SMP
        
        dataset.smp.lag5 <- dataset.smp[, c("Trade_Date", "delivery_date", 
            "delivery_hour", "delivery_interval")]
        dataset.smp.lag5$delivery_date <- dataset.smp$delivery_date + 
            5
        dataset.smp.lag5$Trade_Date <- dataset.smp$Trade_Date + 
            5
        dataset.smp.lag5$SMP_D_Minus_5_Euro <- dataset.smp$SMP
        
        dataset.smp.lag6 <- dataset.smp[, c("Trade_Date", "delivery_date", 
                                            "delivery_hour", "delivery_interval")]
        dataset.smp.lag6$delivery_date <- dataset.smp$delivery_date + 
          6
        dataset.smp.lag6$Trade_Date <- dataset.smp$Trade_Date + 
          6
        dataset.smp.lag6$SMP_D_Minus_6_Euro <- dataset.smp$SMP
        
        dataset.smp.output <- dataset.smp[, c("Trade_Date", "delivery_date", 
            "delivery_hour", "delivery_interval")]
        dataset.smp.output$delivery_date <- dataset.smp$delivery_date - 
            1
        dataset.smp.output$Trade_Date <- dataset.smp$Trade_Date - 
            1
        dataset.smp.output$SMP_Output_Euro <- dataset.smp$SMP
        
        smp.part <- inner_join(dataset.smp, dataset.smp.lag1, 
            by = c("Trade_Date", "delivery_date", "delivery_hour", 
                "delivery_interval"))
        smp.part <- inner_join(smp.part, dataset.smp.lag2, by = c("Trade_Date", 
            "delivery_date", "delivery_hour", "delivery_interval"))
        smp.part <- inner_join(smp.part, dataset.smp.lag3, by = c("Trade_Date", 
            "delivery_date", "delivery_hour", "delivery_interval"))
        smp.part <- inner_join(smp.part, dataset.smp.lag4, by = c("Trade_Date", 
            "delivery_date", "delivery_hour", "delivery_interval"))
        smp.part <- inner_join(smp.part, dataset.smp.lag5, by = c("Trade_Date", 
            "delivery_date", "delivery_hour", "delivery_interval"))
        smp.part <- inner_join(smp.part, dataset.smp.lag6, by = c("Trade_Date",
            "delivery_date", "delivery_hour", "delivery_interval"))
        
        smp.part <- inner_join(smp.part, dataset.smp.output, 
            by = c("Trade_Date", "delivery_date", "delivery_hour", 
                "delivery_interval"))
        
        smp.part$Trade_Date <- smp.part$Trade_Date - 1
        smp.part$delivery_date <- smp.part$delivery_date + 1
        colnames(smp.part) <- c("Report_Date", "Delivery_Date", 
            "Delivery_Hour", "Delivery_Interval", "SMP_D_Euro", 
            "SMP_D_Minus_1_Euro", "SMP_D_Minus_2_Euro", "SMP_D_Minus_3_Euro", 
            "SMP_D_Minus_4_Euro", "SMP_D_Minus_5_Euro", "SMP_D_Minus_6_Euro", "Output_SMP_Euro")
        
        sqlquery.loadDemand <- paste("SELECT [REPORT_DATE],[TRADE_DATE],[JURISDICTION],[DELIVERY_DATE],[DELIVERY_HOUR],[DELIVERY_INTERVAL],[FORECAST_MW] FROM [dbo].[WA_SEMO_FourDayLoadForecast] where [report_date] between '", 
            start, "' and '", end, "' order by report_date asc, delivery_hour asc, delivery_interval asc")
        load.part <- sqlQuery(conn, sqlquery.loadDemand)
        
        load.part <- load.part %>% arrange(REPORT_DATE)
        load.part$REPORT_DATE <- as.Date(as.character(load.part$REPORT_DATE))
        load.part <- load.part %>% group_by(REPORT_DATE, DELIVERY_DATE, 
            DELIVERY_HOUR, DELIVERY_INTERVAL, JURISDICTION) %>% 
            summarise(loadDemand = first(FORECAST_MW))
        load.part <- load.part %>% group_by(REPORT_DATE, DELIVERY_DATE, 
            DELIVERY_HOUR, DELIVERY_INTERVAL) %>% summarise(loadDemand = sum(loadDemand))
        
        load.part$DELIVERY_DATE <- as.Date(as.character(load.part$DELIVERY_DATE))
        colnames(load.part) <- c("Report_Date", "Delivery_Date", 
            "Delivery_Hour", "Delivery_Interval", "LoadDemand")
        
        sqlquery.power <- paste("SELECT [REPORT_DATE],[DELIVERY_DATE],[REGIONS],[SOLARPOWER_MW],[WINDPOWER_MW] FROM [dbo].[WA_Metagroup_RenewablePowerProduction] where [report_date] between '", 
            start, "' and '", end, "' order by report_date asc")
        power.part <- sqlQuery(conn, sqlquery.power)
        
        power.part <- power.part %>% mutate(Power = SOLARPOWER_MW + 
            WINDPOWER_MW) %>% arrange(REPORT_DATE, DELIVERY_DATE, 
            REGIONS)
        power.part$REPORT_DATE <- as.Date(as.character(power.part$REPORT_DATE))
        
        power.part.ireland <- power.part %>% filter(REGIONS != 
            "UK")
        power.part.ireland <- power.part.ireland %>% group_by(REPORT_DATE, 
            DELIVERY_DATE) %>% summarise(Power = sum(Power)) %>% 
            separate(DELIVERY_DATE, c("DELIVERY_DATE", "DELIVERY_HOUR"), 
                sep = " ")
        power.part.ireland$DELIVERY_HOUR <- format(strptime(power.part.ireland$DELIVERY_HOUR, 
            format = "%H:%M:%S"), format = "%H")
        power.part.ireland$DELIVERY_HOUR <- as.numeric(power.part.ireland$DELIVERY_HOUR) + 
            1
        power.part.ireland$DELIVERY_DATE <- as.Date(as.character(power.part.ireland$DELIVERY_DATE))
        colnames(power.part.ireland) <- c("Report_Date", "Delivery_Date", 
            "Delivery_Hour", "Power_Production_Ireland")
        
        power.part.UK <- power.part %>% filter(REGIONS == "UK")
        power.part.UK <- power.part.UK %>% group_by(REPORT_DATE, 
            DELIVERY_DATE) %>% summarise(Power = sum(Power)) %>% 
            separate(DELIVERY_DATE, c("DELIVERY_DATE", "DELIVERY_HOUR"), 
                sep = " ")
        power.part.UK$DELIVERY_HOUR <- format(strptime(power.part.UK$DELIVERY_HOUR, 
            format = "%H:%M:%S"), format = "%H")
        power.part.UK$DELIVERY_HOUR <- as.numeric(power.part.UK$DELIVERY_HOUR) + 
            1
        power.part.UK$DELIVERY_DATE <- as.Date(as.character(power.part.UK$DELIVERY_DATE))
        colnames(power.part.UK) <- c("Report_Date", "Delivery_Date", 
            "Delivery_Hour", "Power_Production_UK")
        
        dataset.temp <- inner_join(smp.part, load.part, by = c("Report_Date", 
            "Delivery_Date", "Delivery_Hour", "Delivery_Interval"))
        dataset.temp <- inner_join(dataset.temp, power.part.ireland, 
            by = c("Report_Date", "Delivery_Date", "Delivery_Hour"))
        dataset.temp <- inner_join(dataset.temp, power.part.UK, 
            by = c("Report_Date", "Delivery_Date", "Delivery_Hour"))
        
        dataset.temp <- data.frame(Report_Date = dataset.temp$Report_Date, Delivery_Date = dataset.temp$Delivery_Date, 
                Delivery_Hour = dataset.temp$Delivery_Hour, Delivery_Interval = dataset.temp$Delivery_Interval, 
                SMP_D_Euro = dataset.temp$SMP_D_Euro, SMP_D_Minus_1_Euro = dataset.temp$SMP_D_Minus_1_Euro, 
                SMP_D_Minus_2_Euro = dataset.temp$SMP_D_Minus_2_Euro, 
                SMP_D_Minus_3_Euro = dataset.temp$SMP_D_Minus_3_Euro, 
                SMP_D_Minus_4_Euro = dataset.temp$SMP_D_Minus_4_Euro, 
                SMP_D_Minus_5_Euro = dataset.temp$SMP_D_Minus_5_Euro, 
                SMP_D_Minus_6_Euro = dataset.temp$SMP_D_Minus_6_Euro, 
                LoadDemand = dataset.temp$LoadDemand, Power_Production_Ireland = dataset.temp$Power_Production_Ireland, 
                Power_Production_UK = dataset.temp$Power_Production_UK, 
                Output_SMP_Euro = dataset.temp$Output_SMP_Euro)
    
        dataset.train <- dataset.temp %>% filter(Report_Date >= start.train, 
                                                 Report_Date <= end.train) %>% select(-Report_Date)
        dataset.test <- dataset.temp %>% filter(Report_Date >= start.test, 
                                                Report_Date <= end.test) %>% select(-Report_Date)
    
    varnames <- colnames(dataset.train)
    jsonVar <- serializeJSON(varnames)
    
    method <- list(Name = "None", Description = "Do Nothing")
    
    input <- list(SQL_Query = jsonVar, Training_Testing_Rate = rate, 
        Input_Discription = Input_Discription, StartDate_Training = start.train, 
        EndDate_Training = end.train, StartDate_Testing = start.test, 
        EndDate_Testing = end.test)
    
    return(list(dataset.train = dataset.train, dataset.test = dataset.test, 
        input = input, preprocessing = method))
}

DatasetAquisition.UKPowerProduction <- function(conn, start.train = (Sys.Date() - 
    15), end.train = Sys.Date(), start.test = (Sys.Date() - 15), 
    end.test = Sys.Date()) {
    require(RODBC)
    source("~/FusionProject/R Code/FUSION_HyFIS/DatasetBasedOnSlope.R")
    source("~/FusionProject/R Code/FUSION_HyFIS/InsertDatabase.R")
    source("~/FusionProject/R Code/FUSION_HyFIS/SelectDatabase.R")
    
    ## Training Dataset
    sqlquery.train <- paste("SELECT [delivery_date],[delivery_hour],[delivery_interval],[NI_Load_D_MWh],[NI_Load_D_plus_1_MWh],[ROI_Load_D_MWh],[ROI_Load_D_plus_1_MWh],[SMP_D_EURO],[SMP_D_Minus_1_EURO],[SMP_D_Minus_2_EURO],[SMP_D_Minus_3_EURO],[SMP_D_Minus_4_EURO],[SMP_D_Minus_5_EURO],[SMP_D_Minus_6_EURO],[SMP_D_Minus_7_EURO],[NI_Solar_D_MW],[NI_Solar_D_Plus_1_MW],[ROI_Solar_D_MW],[ROI_Solar_D_Plus_1_MW],[UK_Solar_D_Plus_1_MW],[NI_Wind_D_MW],[NI_Wind_D_Plus_1_MW],[ROI_Wind_D_MW],[ROI_Wind_D_Plus_1_MW],[UK_Wind_D_Plus_1_MW],[SMP_D_Plus_2_Output_Euro] FROM [dbo].[dataset] where [delivery_date] between '", 
        start.train, "' and '", end.train, "' order by delivery_date asc, delivery_hour asc, delivery_interval asc")
    
    dataset.train <- sqlQuery(conn, sqlquery.train)
    
    # dataset <- dataset[complete.cases(dataset),]
    
    if (is.null(dataset.train) || nrow(dataset.train) == 0) 
        return(NULL)
    dataset.train[is.null(dataset.train)] <- 0
    dataset.train[is.na(dataset.train)] <- 0
    
    NI_Load_D_plus_1_MWh <- dataset.train$NI_Load_D_plus_1_MWh
    
    ROI_Load_D_plus_1_MWh <- dataset.train$ROI_Load_D_plus_1_MWh
    
    Load_D_plus_1_MWh <- NI_Load_D_plus_1_MWh + ROI_Load_D_plus_1_MWh
    
    NI_Solar_D_plus_1_MW <- dataset.train$NI_Solar_D_Plus_1_MW
    
    NI_Wind_D_plus_1_MW <- dataset.train$NI_Wind_D_Plus_1_MW
    
    ROI_Wind_D_plus_1_MW <- dataset.train$ROI_Wind_D_Plus_1_MW
    
    Power_D_plus_1_MW <- NI_Solar_D_plus_1_MW + NI_Wind_D_plus_1_MW + 
        ROI_Wind_D_plus_1_MW
    
    UK_Power_D_Plus_1_MW <- dataset.train$UK_Solar_D_Plus_1_MW + 
        dataset.train$UK_Wind_D_Plus_1_MW
    
    SMP_D_EURO <- dataset.train$SMP_D_EURO
    SMP_D_Minus_1_EURO <- dataset.train$SMP_D_Minus_1_EURO
    SMP_D_Minus_2_EURO <- dataset.train$SMP_D_Minus_2_EURO
    SMP_D_Minus_3_EURO <- dataset.train$SMP_D_Minus_3_EURO
    SMP_D_Minus_4_EURO <- dataset.train$SMP_D_Minus_4_EURO
    SMP_D_Minus_5_EURO <- dataset.train$SMP_D_Minus_5_EURO
    SMP_D_Minus_6_EURO <- dataset.train$SMP_D_Minus_6_EURO
    SMP_D_Minus_7_EURO <- dataset.train$SMP_D_Minus_7_EURO
    
    SMP_D_Plus_2_Output_Euro <- dataset.train$SMP_D_Plus_2_Output_Euro
    
    dataset.train <- cbind(dataset.train[, 1:3], Load_D_plus_1_MWh, 
        SMP_D_EURO, SMP_D_Minus_1_EURO, SMP_D_Minus_2_EURO, SMP_D_Minus_3_EURO, 
        SMP_D_Minus_4_EURO, SMP_D_Minus_5_EURO, Power_D_plus_1_MW, 
        UK_Power_D_Plus_1_MW, SMP_D_Plus_2_Output_Euro)
    
    ## Testing Dataset
    sqlquery.test <- paste("SELECT [delivery_date],[delivery_hour],[delivery_interval],[NI_Load_D_MWh],[NI_Load_D_plus_1_MWh],[ROI_Load_D_MWh],[ROI_Load_D_plus_1_MWh],[SMP_D_EURO],[SMP_D_Minus_1_EURO],[SMP_D_Minus_2_EURO],[SMP_D_Minus_3_EURO],[SMP_D_Minus_4_EURO],[SMP_D_Minus_5_EURO],[SMP_D_Minus_6_EURO],[SMP_D_Minus_7_EURO],[NI_Solar_D_MW],[NI_Solar_D_Plus_1_MW],[ROI_Solar_D_MW],[ROI_Solar_D_Plus_1_MW],[UK_Solar_D_Plus_1_MW],[NI_Wind_D_MW],[NI_Wind_D_Plus_1_MW],[ROI_Wind_D_MW],[ROI_Wind_D_Plus_1_MW],[UK_Wind_D_Plus_1_MW],[SMP_D_Plus_2_Output_Euro] FROM [dbo].[dataset] where [delivery_date] between '", 
        start.test, "' and '", end.test, "' order by delivery_date asc, delivery_hour asc, delivery_interval asc")
    
    dataset.test <- sqlQuery(conn, sqlquery.test)
    
    # dataset <- dataset[complete.cases(dataset),]
    
    if (is.null(dataset.test) || nrow(dataset.test) == 0) 
        return(NULL)
    dataset.test[is.null(dataset.test)] <- 0
    dataset.test[is.na(dataset.test)] <- 0
    
    NI_Load_D_plus_1_MWh <- dataset.test$NI_Load_D_plus_1_MWh
    
    ROI_Load_D_plus_1_MWh <- dataset.test$ROI_Load_D_plus_1_MWh
    
    Load_D_plus_1_MWh <- NI_Load_D_plus_1_MWh + ROI_Load_D_plus_1_MWh
    
    NI_Solar_D_plus_1_MW <- dataset.test$NI_Solar_D_Plus_1_MW
    
    NI_Wind_D_plus_1_MW <- dataset.test$NI_Wind_D_Plus_1_MW
    
    ROI_Wind_D_plus_1_MW <- dataset.test$ROI_Wind_D_Plus_1_MW
    
    Power_D_plus_1_MW <- NI_Solar_D_plus_1_MW + NI_Wind_D_plus_1_MW + 
        ROI_Wind_D_plus_1_MW
    
    UK_Power_D_Plus_1_MW <- dataset.test$UK_Solar_D_Plus_1_MW + 
        dataset.test$UK_Wind_D_Plus_1_MW
    
    SMP_D_EURO <- dataset.test$SMP_D_EURO
    SMP_D_Minus_1_EURO <- dataset.test$SMP_D_Minus_1_EURO
    SMP_D_Minus_2_EURO <- dataset.test$SMP_D_Minus_2_EURO
    SMP_D_Minus_3_EURO <- dataset.test$SMP_D_Minus_3_EURO
    SMP_D_Minus_4_EURO <- dataset.test$SMP_D_Minus_4_EURO
    SMP_D_Minus_5_EURO <- dataset.test$SMP_D_Minus_5_EURO
    SMP_D_Minus_6_EURO <- dataset.test$SMP_D_Minus_6_EURO
    SMP_D_Minus_7_EURO <- dataset.test$SMP_D_Minus_7_EURO
    
    SMP_D_Plus_2_Output_Euro <- dataset.test$SMP_D_Plus_2_Output_Euro
    
    dataset.test <- cbind(dataset.test[, 1:3], Load_D_plus_1_MWh, 
        SMP_D_EURO, SMP_D_Minus_1_EURO, SMP_D_Minus_2_EURO, SMP_D_Minus_3_EURO, 
        SMP_D_Minus_4_EURO, SMP_D_Minus_5_EURO, Power_D_plus_1_MW, 
        UK_Power_D_Plus_1_MW, SMP_D_Plus_2_Output_Euro)
    
    method <- list(Name = "None", Description = "Do Nothing")
    
    input <- list(SQL_Query = sqlquery.train, Training_Testing_Rate = rate, 
        Input_Discription = Input_Discription, StartDate_Training = start.train, 
        EndDate_Training = end.train, StartDate_Testing = start.test, 
        EndDate_Testing = end.test)
    
    return(list(dataset.train = dataset.train, dataset.test = dataset.test, 
        input = input, preprocessing = method))
}

DatasetAquisition.WithoutPowerProduction <- function(conn, start.train = (Sys.Date() - 
    15), end.train = Sys.Date(), start.test = (Sys.Date() - 15), 
    end.test = Sys.Date()) {
    
    start <- min(c(start.train, start.test, end.train, end.test))
    end <- max(c(start.train, start.test, end.train, end.test))
    sqlquery <- paste("SELECT [Trade_Date],[delivery_date],[delivery_hour],[delivery_interval],[SMP] FROM [dbo].[WA_SEMO_MarketResults] where [delivery_date] between '", 
        (start - 20), "' and '", (end + 1), "' order by delivery_date asc, delivery_hour asc, delivery_interval asc")
    dataset.smp <- sqlQuery(conn, sqlquery)
    dataset.smp[dataset.smp < 0] <- 0
    dataset.smp$delivery_date <- as.Date(as.character(dataset.smp$delivery_date))
    dataset.smp$Trade_Date <- as.Date(as.character(dataset.smp$Trade_Date))
    
    dataset.smp.lag7 <- dataset.smp
    dataset.smp.lag7$delivery_date <- dataset.smp$delivery_date + 
        6
    dataset.smp.lag7$Trade_Date <- dataset.smp$Trade_Date + 6
    dataset.smp.lag14 <- dataset.smp
    dataset.smp.lag14$delivery_date <- dataset.smp$delivery_date + 
        13
    dataset.smp.lag14$Trade_Date <- dataset.smp$Trade_Date + 
        13
    
    dataset.smp.hhlag1 <- dataset.smp[, c("Trade_Date", "delivery_date", 
        "delivery_hour", "delivery_interval")]
    dataset.smp.hhlag1$SMP_HH_Minus_1_Euro <- c(NA, dataset.smp$SMP[1:(nrow(dataset.smp) - 
        1)])
    
    dataset.smp.hhlag2 <- dataset.smp[, c("Trade_Date", "delivery_date", 
        "delivery_hour", "delivery_interval")]
    dataset.smp.hhlag2$SMP_HH_Minus_2_Euro <- c(NA, NA, dataset.smp$SMP[1:(nrow(dataset.smp) - 
        2)])
    
    dataset.smp.output <- dataset.smp
    dataset.smp.output$delivery_date <- dataset.smp$delivery_date - 
        1
    dataset.smp.output$Trade_Date <- dataset.smp$Trade_Date - 
        1
    
    smp.part <- inner_join(dataset.smp, dataset.smp.lag7, by = c("Trade_Date", 
        "delivery_date", "delivery_hour", "delivery_interval"))
    smp.part <- inner_join(smp.part, dataset.smp.lag14, by = c("Trade_Date", 
        "delivery_date", "delivery_hour", "delivery_interval"))
    smp.part <- inner_join(smp.part, dataset.smp.hhlag1, by = c("Trade_Date", 
        "delivery_date", "delivery_hour", "delivery_interval"))
    smp.part <- inner_join(smp.part, dataset.smp.hhlag2, by = c("Trade_Date", 
        "delivery_date", "delivery_hour", "delivery_interval"))
    smp.part <- inner_join(smp.part, dataset.smp.output, by = c("Trade_Date", 
        "delivery_date", "delivery_hour", "delivery_interval"))
    
    smp.part$Trade_Date <- smp.part$Trade_Date - 1
    smp.part$delivery_date <- smp.part$delivery_date + 1
    colnames(smp.part) <- c("Report_Date", "Delivery_Date", "Delivery_Hour", 
        "Delivery_Interval", "SMP_D_Euro", "SMP_D_Minus_6_Euro", 
        "SMP_D_Minus_13_Euro","SMP_HH_Minus_1_Euro","SMP_HH_Minus_2_Euro", "Output_SMP_Euro")
    
    sqlquery.loadDemand <- paste("SELECT [REPORT_DATE],[TRADE_DATE],[JURISDICTION],[DELIVERY_DATE],[DELIVERY_HOUR],[DELIVERY_INTERVAL],[FORECAST_MW] FROM [dbo].[WA_SEMO_FourDayLoadForecast] where [report_date] between '", 
        start, "' and '", end, "' order by report_date asc, delivery_hour asc, delivery_interval asc")
    load.part <- sqlQuery(conn, sqlquery.loadDemand)
    
    load.part <- load.part %>% arrange(REPORT_DATE)
    load.part$REPORT_DATE <- as.Date(as.character(load.part$REPORT_DATE))
    load.part <- load.part %>% group_by(REPORT_DATE, DELIVERY_DATE, 
        DELIVERY_HOUR, DELIVERY_INTERVAL, JURISDICTION) %>% summarise(loadDemand = first(FORECAST_MW))
    load.part <- load.part %>% group_by(REPORT_DATE, DELIVERY_DATE, 
        DELIVERY_HOUR, DELIVERY_INTERVAL) %>% summarise(loadDemand = sum(loadDemand))
    
    load.part$DELIVERY_DATE <- as.Date(as.character(load.part$DELIVERY_DATE))
    colnames(load.part) <- c("Report_Date", "Delivery_Date", 
        "Delivery_Hour", "Delivery_Interval", "LoadDemand")
    
    dataset.temp <- inner_join(smp.part, load.part, by = c("Report_Date", 
        "Delivery_Date", "Delivery_Hour", "Delivery_Interval"))
    
    
    dataset.temp <- data.frame(Report_Date = dataset.temp$Report_Date, 
        Delivery_Date = dataset.temp$Delivery_Date, Delivery_Hour = dataset.temp$Delivery_Hour, 
        Delivery_Interval = dataset.temp$Delivery_Interval, SMP_D_Euro = dataset.temp$SMP_D_Euro, 
        SMP_D_Minus_6_Euro = dataset.temp$SMP_D_Minus_6_Euro, 
        SMP_D_Minus_13_Euro = dataset.temp$SMP_D_Minus_13_Euro, 
        SMP_HH_Minus_1_Euro = dataset.temp$SMP_HH_Minus_1_Euro, 
        SMP_HH_Minus_2_Euro = dataset.temp$SMP_HH_Minus_2_Euro, 
        LoadDemand = dataset.temp$LoadDemand, Output_SMP_Euro = dataset.temp$Output_SMP_Euro)
    
    dataset.train <- dataset.temp %>% filter(Report_Date >= start.train, 
        Report_Date <= end.train) %>% select(-Report_Date)
    dataset.test <- dataset.temp %>% filter(Report_Date >= start.test, 
        Report_Date <= end.test) %>% select(-Report_Date)
    
    
    varnames <- colnames(dataset.train)
    jsonVar <- serializeJSON(varnames)
    
    method <- list(Name = "None", Description = "Do Nothing")
    
    input <- list(SQL_Query = jsonVar, Training_Testing_Rate = rate, 
        Input_Discription = Input_Discription, StartDate_Training = start.train, 
        EndDate_Training = end.train, StartDate_Testing = start.test, 
        EndDate_Testing = end.test)
    
    Input_Discription <- "Dataset of Weekly Value without Power Production: Dhh, D-7hh, D-14hh without Power Production"
    
    return(list(dataset.train = dataset.train, dataset.test = dataset.test, 
        input = input, preprocessing = method, Input_Discription = Input_Discription))
}

DatasetAquisition.WeeklyOfDayWithUK <- function(conn, start.train = (Sys.Date() - 
    15), end.train = Sys.Date(), start.test = (Sys.Date() - 15), 
    end.test = Sys.Date()) {
    
  start <- min(c(start.train, start.test, end.train, end.test))
  end <- max(c(start.train, start.test, end.train, end.test))
        sqlquery <- paste("SELECT [Trade_Date],[delivery_date],[delivery_hour],[delivery_interval],[SMP] FROM [dbo].[WA_SEMO_MarketResults] where [delivery_date] between '", 
            (start - 20), "' and '", (end + 1), "' order by delivery_date asc, delivery_hour asc, delivery_interval asc")
        dataset.smp <- sqlQuery(conn, sqlquery)
        dataset.smp[dataset.smp < 0] <- 0
        dataset.smp$delivery_date <- as.Date(as.character(dataset.smp$delivery_date))
        dataset.smp$Trade_Date <- as.Date(as.character(dataset.smp$Trade_Date))
        
        dataset.smp.lag7 <- dataset.smp
        dataset.smp.lag7$delivery_date <- dataset.smp$delivery_date + 
            6
        dataset.smp.lag7$Trade_Date <- dataset.smp$Trade_Date + 
            6
        dataset.smp.lag14 <- dataset.smp
        dataset.smp.lag14$delivery_date <- dataset.smp$delivery_date + 
            13
        dataset.smp.lag14$Trade_Date <- dataset.smp$Trade_Date + 
            13
        dataset.smp.output <- dataset.smp
        dataset.smp.output$delivery_date <- dataset.smp$delivery_date - 
            1
        dataset.smp.output$Trade_Date <- dataset.smp$Trade_Date - 
            1
        
        smp.part <- inner_join(dataset.smp, dataset.smp.lag7, 
            by = c("Trade_Date", "delivery_date", "delivery_hour", 
                "delivery_interval"))
        smp.part <- inner_join(smp.part, dataset.smp.lag14, by = c("Trade_Date", 
            "delivery_date", "delivery_hour", "delivery_interval"))
        smp.part <- inner_join(smp.part, dataset.smp.output, 
            by = c("Trade_Date", "delivery_date", "delivery_hour", 
                "delivery_interval"))
        
        smp.part$Trade_Date <- smp.part$Trade_Date - 1
        smp.part$delivery_date <- smp.part$delivery_date + 1
        colnames(smp.part) <- c("Report_Date", "Delivery_Date", 
            "Delivery_Hour", "Delivery_Interval", "SMP_D_Euro", 
            "SMP_D_Minus_6_Euro", "SMP_D_Minus_13_Euro", "Output_SMP_Euro")
        
        sqlquery.loadDemand <- paste("SELECT [REPORT_DATE],[TRADE_DATE],[JURISDICTION],[DELIVERY_DATE],[DELIVERY_HOUR],[DELIVERY_INTERVAL],[FORECAST_MW] FROM [dbo].[WA_SEMO_FourDayLoadForecast] where [report_date] between '", 
            start, "' and '", end, "' order by report_date asc, delivery_hour asc, delivery_interval asc")
        load.part <- sqlQuery(conn, sqlquery.loadDemand)
        
        load.part <- load.part %>% arrange(REPORT_DATE)
        load.part$REPORT_DATE <- as.Date(as.character(load.part$REPORT_DATE))
        load.part <- load.part %>% group_by(REPORT_DATE, DELIVERY_DATE, 
            DELIVERY_HOUR, DELIVERY_INTERVAL, JURISDICTION) %>% 
            summarise(loadDemand = first(FORECAST_MW))
        load.part <- load.part %>% group_by(REPORT_DATE, DELIVERY_DATE, 
            DELIVERY_HOUR, DELIVERY_INTERVAL) %>% summarise(loadDemand = sum(loadDemand))
        
        load.part$DELIVERY_DATE <- as.Date(as.character(load.part$DELIVERY_DATE))
        colnames(load.part) <- c("Report_Date", "Delivery_Date", 
            "Delivery_Hour", "Delivery_Interval", "LoadDemand")
        
        sqlquery.power <- paste("SELECT [REPORT_DATE],[DELIVERY_DATE],[REGIONS],[SOLARPOWER_MW],[WINDPOWER_MW] FROM [dbo].[WA_Metagroup_RenewablePowerProduction] where [report_date] between '", 
            start, "' and '", end, "' order by report_date asc")
        power.part <- sqlQuery(conn, sqlquery.power)
        
        power.part <- power.part %>% mutate(Power = SOLARPOWER_MW + 
            WINDPOWER_MW) %>% arrange(REPORT_DATE, DELIVERY_DATE, 
            REGIONS)
        power.part$REPORT_DATE <- as.Date(as.character(power.part$REPORT_DATE))
        
        power.part.ireland <- power.part %>% filter(REGIONS != 
            "UK")
        power.part.ireland <- power.part.ireland %>% group_by(REPORT_DATE, 
            DELIVERY_DATE) %>% summarise(Power = sum(Power)) %>% 
            separate(DELIVERY_DATE, c("DELIVERY_DATE", "DELIVERY_HOUR"), 
                sep = " ")
        power.part.ireland$DELIVERY_HOUR <- format(strptime(power.part.ireland$DELIVERY_HOUR, 
            format = "%H:%M:%S"), format = "%H")
        power.part.ireland$DELIVERY_HOUR <- as.numeric(power.part.ireland$DELIVERY_HOUR) + 
            1
        power.part.ireland$DELIVERY_DATE <- as.Date(as.character(power.part.ireland$DELIVERY_DATE))
        colnames(power.part.ireland) <- c("Report_Date", "Delivery_Date", 
            "Delivery_Hour", "Power_Production_Ireland")
        
        power.part.UK <- power.part %>% filter(REGIONS == "UK")
        power.part.UK <- power.part.UK %>% group_by(REPORT_DATE, 
            DELIVERY_DATE) %>% summarise(Power = sum(Power)) %>% 
            separate(DELIVERY_DATE, c("DELIVERY_DATE", "DELIVERY_HOUR"), 
                sep = " ")
        power.part.UK$DELIVERY_HOUR <- format(strptime(power.part.UK$DELIVERY_HOUR, 
            format = "%H:%M:%S"), format = "%H")
        power.part.UK$DELIVERY_HOUR <- as.numeric(power.part.UK$DELIVERY_HOUR) + 
            1
        power.part.UK$DELIVERY_DATE <- as.Date(as.character(power.part.UK$DELIVERY_DATE))
        colnames(power.part.UK) <- c("Report_Date", "Delivery_Date", 
            "Delivery_Hour", "Power_Production_UK")
        
        dataset.temp <- inner_join(smp.part, load.part, by = c("Report_Date", 
            "Delivery_Date", "Delivery_Hour", "Delivery_Interval"))
        dataset.temp <- inner_join(dataset.temp, power.part.ireland, 
            by = c("Report_Date", "Delivery_Date", "Delivery_Hour"))
        dataset.temp <- inner_join(dataset.temp, power.part.UK, 
            by = c("Report_Date", "Delivery_Date", "Delivery_Hour"))
    
        dataset.temp <- data.frame(Report_Date = dataset.temp$Report_Date,Delivery_Date = dataset.temp$Delivery_Date, 
            Delivery_Hour = dataset.temp$Delivery_Hour, Delivery_Interval = dataset.temp$Delivery_Interval, 
            SMP_D_Euro = dataset.temp$SMP_D_Euro, SMP_D_Minus_6_Euro = dataset.temp$SMP_D_Minus_6_Euro, 
            SMP_D_Minus_13_Euro = dataset.temp$SMP_D_Minus_13_Euro, 
            LoadDemand = dataset.temp$LoadDemand, Power_Production_Ireland = dataset.temp$Power_Production_Ireland, 
            Power_Production_UK = dataset.temp$Power_Production_UK, 
            Output_SMP_Euro = dataset.temp$Output_SMP_Euro)
        
        dataset.train <- dataset.temp %>% filter(Report_Date >= start.train, 
                                                 Report_Date <= end.train) %>% select(-Report_Date)
        dataset.test <- dataset.temp %>% filter(Report_Date >= start.test, 
                                                Report_Date <= end.test) %>% select(-Report_Date)
        
    varnames <- colnames(dataset.train)
    jsonVar <- serializeJSON(varnames)
    
    method <- list(Name = "None", Description = "Do Nothing")
    
    input <- list(SQL_Query = jsonVar, Training_Testing_Rate = rate, 
        Input_Discription = Input_Discription, StartDate_Training = start.train, 
        EndDate_Training = end.train, StartDate_Testing = start.test, 
        EndDate_Testing = end.test)
    
    Input_Discription <- "Dataset of Weekly Value with UK Power Production: Dhh, D-7hh, D-14hh with UK"
    
    return(list(dataset.train = dataset.train, dataset.test = dataset.test, 
        input = input, preprocessing = method, Input_Discription = Input_Discription))
}

DatasetAquisition.WeeklyOfDayWithoutUK <- function(conn, start.train = (Sys.Date() - 
    15), end.train = Sys.Date(), start.test = (Sys.Date() - 15), 
    end.test = Sys.Date()) {
    
    for (i in 1:2) {
        
        if (i == 1) {
            start <- start.train
            end <- end.train
        } else {
            start <- start.test
            end <- end.test
        }
        sqlquery <- paste("SELECT [Trade_Date],[delivery_date],[delivery_hour],[delivery_interval],[SMP] FROM [dbo].[WA_SEMO_MarketResults] where [delivery_date] between '", 
            (start - 20), "' and '", (end + 1), "' order by delivery_date asc, delivery_hour asc, delivery_interval asc")
        dataset.smp <- sqlQuery(conn, sqlquery)
        dataset.smp[dataset.smp < 0] <- 0
        dataset.smp$delivery_date <- as.Date(as.character(dataset.smp$delivery_date))
        dataset.smp$Trade_Date <- as.Date(as.character(dataset.smp$Trade_Date))
        
        dataset.smp.lag7 <- dataset.smp
        dataset.smp.lag7$delivery_date <- dataset.smp$delivery_date + 
            6
        dataset.smp.lag7$Trade_Date <- dataset.smp$Trade_Date + 
            6
        dataset.smp.lag14 <- dataset.smp
        dataset.smp.lag14$delivery_date <- dataset.smp$delivery_date + 
            13
        dataset.smp.lag14$Trade_Date <- dataset.smp$Trade_Date + 
            13
        dataset.smp.output <- dataset.smp
        dataset.smp.output$delivery_date <- dataset.smp$delivery_date - 
            1
        dataset.smp.output$Trade_Date <- dataset.smp$Trade_Date - 
            1
        
        smp.part <- inner_join(dataset.smp, dataset.smp.lag7, 
            by = c("Trade_Date", "delivery_date", "delivery_hour", 
                "delivery_interval"))
        smp.part <- inner_join(smp.part, dataset.smp.lag14, by = c("Trade_Date", 
            "delivery_date", "delivery_hour", "delivery_interval"))
        smp.part <- inner_join(smp.part, dataset.smp.output, 
            by = c("Trade_Date", "delivery_date", "delivery_hour", 
                "delivery_interval"))
        
        smp.part$Trade_Date <- smp.part$Trade_Date - 1
        smp.part$delivery_date <- smp.part$delivery_date + 1
        colnames(smp.part) <- c("Report_Date", "Delivery_Date", 
            "Delivery_Hour", "Delivery_Interval", "SMP_D_Euro", 
            "SMP_D_Minus_6_Euro", "SMP_D_Minus_13_Euro", "Output_SMP_Euro")
        
        sqlquery.loadDemand <- paste("SELECT [REPORT_DATE],[TRADE_DATE],[JURISDICTION],[DELIVERY_DATE],[DELIVERY_HOUR],[DELIVERY_INTERVAL],[FORECAST_MW] FROM [dbo].[WA_SEMO_FourDayLoadForecast] where [report_date] between '", 
            start, "' and '", end, "' order by report_date asc, delivery_hour asc, delivery_interval asc")
        load.part <- sqlQuery(conn, sqlquery.loadDemand)
        
        load.part <- load.part %>% arrange(REPORT_DATE)
        load.part$REPORT_DATE <- as.Date(as.character(load.part$REPORT_DATE))
        load.part <- load.part %>% group_by(REPORT_DATE, DELIVERY_DATE, 
            DELIVERY_HOUR, DELIVERY_INTERVAL, JURISDICTION) %>% 
            summarise(loadDemand = first(FORECAST_MW))
        load.part <- load.part %>% group_by(REPORT_DATE, DELIVERY_DATE, 
            DELIVERY_HOUR, DELIVERY_INTERVAL) %>% summarise(loadDemand = sum(loadDemand))
        
        load.part$DELIVERY_DATE <- as.Date(as.character(load.part$DELIVERY_DATE))
        colnames(load.part) <- c("Report_Date", "Delivery_Date", 
            "Delivery_Hour", "Delivery_Interval", "LoadDemand")
        
        sqlquery.power <- paste("SELECT [REPORT_DATE],[DELIVERY_DATE],[REGIONS],[SOLARPOWER_MW],[WINDPOWER_MW] FROM [dbo].[WA_Metagroup_RenewablePowerProduction] where [report_date] between '", 
            start, "' and '", end, "' order by report_date asc")
        power.part <- sqlQuery(conn, sqlquery.power)
        
        power.part <- power.part %>% mutate(Power = SOLARPOWER_MW + 
            WINDPOWER_MW) %>% arrange(REPORT_DATE, DELIVERY_DATE, 
            REGIONS)
        power.part$REPORT_DATE <- as.Date(as.character(power.part$REPORT_DATE))
        
        power.part.ireland <- power.part %>% filter(REGIONS != 
            "UK")
        power.part.ireland <- power.part.ireland %>% group_by(REPORT_DATE, 
            DELIVERY_DATE) %>% summarise(Power = sum(Power)) %>% 
            separate(DELIVERY_DATE, c("DELIVERY_DATE", "DELIVERY_HOUR"), 
                sep = " ")
        power.part.ireland$DELIVERY_HOUR <- format(strptime(power.part.ireland$DELIVERY_HOUR, 
            format = "%H:%M:%S"), format = "%H")
        power.part.ireland$DELIVERY_HOUR <- as.numeric(power.part.ireland$DELIVERY_HOUR) + 
            1
        power.part.ireland$DELIVERY_DATE <- as.Date(as.character(power.part.ireland$DELIVERY_DATE))
        colnames(power.part.ireland) <- c("Report_Date", "Delivery_Date", 
            "Delivery_Hour", "Power_Production_Ireland")
        
        dataset.temp <- inner_join(smp.part, load.part, by = c("Report_Date", 
            "Delivery_Date", "Delivery_Hour", "Delivery_Interval"))
        dataset.temp <- inner_join(dataset.temp, power.part.ireland, 
            by = c("Report_Date", "Delivery_Date", "Delivery_Hour"))
        
        if (i == 1) {
            dataset.train <- data.frame(Delivery_Date = dataset.temp$Delivery_Date, 
                Delivery_Hour = dataset.temp$Delivery_Hour, Delivery_Interval = dataset.temp$Delivery_Interval, 
                SMP_D_Euro = dataset.temp$SMP_D_Euro, SMP_D_Minus_6_Euro = dataset.temp$SMP_D_Minus_6_Euro, 
                SMP_D_Minus_13_Euro = dataset.temp$SMP_D_Minus_13_Euro, 
                LoadDemand = dataset.temp$LoadDemand, Power_Production_Ireland = dataset.temp$Power_Production_Ireland, 
                Output_SMP_Euro = dataset.temp$Output_SMP_Euro)
        } else {
            dataset.test <- data.frame(Delivery_Date = dataset.temp$Delivery_Date, 
                Delivery_Hour = dataset.temp$Delivery_Hour, Delivery_Interval = dataset.temp$Delivery_Interval, 
                SMP_D_Euro = dataset.temp$SMP_D_Euro, SMP_D_Minus_6_Euro = dataset.temp$SMP_D_Minus_6_Euro, 
                SMP_D_Minus_13_Euro = dataset.temp$SMP_D_Minus_13_Euro, 
                LoadDemand = dataset.temp$LoadDemand, Power_Production_Ireland = dataset.temp$Power_Production_Ireland, 
                Output_SMP_Euro = dataset.temp$Output_SMP_Euro)
        }
    }
    
    varnames <- colnames(dataset.train)
    jsonVar <- serializeJSON(varnames)
    
    method <- list(Name = "None", Description = "Do Nothing")
    
    input <- list(SQL_Query = jsonVar, Training_Testing_Rate = rate, 
        Input_Discription = Input_Discription, StartDate_Training = start.train, 
        EndDate_Training = end.train, StartDate_Testing = start.test, 
        EndDate_Testing = end.test)
    
    Input_Discription <- "Dataset of Weekly Value without UK Power Production: Dhh, D-7hh, D-14hh without UK"
    return(list(dataset.train = dataset.train, dataset.test = dataset.test, 
        input = input, preprocessing = method, Input_Discription = Input_Discription))
}

DatasetAquisition.WeeklyOfDayPlusHalfHourWithUK <- function(conn, 
    start.train = (Sys.Date() - 15), end.train = Sys.Date(), 
    start.test = (Sys.Date() - 15), end.test = Sys.Date()) {
    
  require(tidyr, quietly = TRUE)
  require(dplyr, quietly = TRUE)
  start <- min(c(start.train, start.test, end.train, end.test))
  end <- max(c(start.train, start.test, end.train, end.test))
        sqlquery <- paste("SELECT [Trade_Date],[delivery_date],[delivery_hour],[delivery_interval],[SMP] FROM [dbo].[WA_SEMO_MarketResults] where [delivery_date] between '", 
            (start - 20), "' and '", (end + 1), "' order by delivery_date asc, delivery_hour asc, delivery_interval asc")
        dataset.smp <- sqlQuery(conn, sqlquery)
        dataset.smp[dataset.smp < 0] <- 0
        dataset.smp$delivery_date <- as.Date(as.character(dataset.smp$delivery_date))
        dataset.smp$Trade_Date <- as.Date(as.character(dataset.smp$Trade_Date))
        
        dataset.smp.lag7 <- dataset.smp
        dataset.smp.lag7$delivery_date <- dataset.smp$delivery_date + 
            6
        dataset.smp.lag7$Trade_Date <- dataset.smp$Trade_Date + 
            6
        
        dataset.smp.lag14 <- dataset.smp
        dataset.smp.lag14$delivery_date <- dataset.smp$delivery_date + 
            13
        dataset.smp.lag14$Trade_Date <- dataset.smp$Trade_Date + 
            13
        
        dataset.smp.hhlag1 <- dataset.smp[, c("Trade_Date", "delivery_date", 
            "delivery_hour", "delivery_interval")]
        dataset.smp.hhlag1$SMP_HH_Minus_1_Euro <- c(NA, dataset.smp$SMP[1:(nrow(dataset.smp) - 
            1)])
        
        dataset.smp.hhlag2 <- dataset.smp[, c("Trade_Date", "delivery_date", 
            "delivery_hour", "delivery_interval")]
        dataset.smp.hhlag2$SMP_HH_Minus_2_Euro <- c(NA, NA, dataset.smp$SMP[1:(nrow(dataset.smp) - 
            2)])
        
        dataset.smp.output <- dataset.smp
        dataset.smp.output$delivery_date <- dataset.smp$delivery_date - 
            1
        dataset.smp.output$Trade_Date <- dataset.smp$Trade_Date - 
            1
        
        smp.part <- inner_join(dataset.smp, dataset.smp.lag7, 
            by = c("Trade_Date", "delivery_date", "delivery_hour", 
                "delivery_interval"))
        smp.part <- inner_join(smp.part, dataset.smp.lag14, by = c("Trade_Date", 
            "delivery_date", "delivery_hour", "delivery_interval"))
        smp.part <- inner_join(smp.part, dataset.smp.hhlag1, 
            by = c("Trade_Date", "delivery_date", "delivery_hour", 
                "delivery_interval"))
        smp.part <- inner_join(smp.part, dataset.smp.hhlag2, 
            by = c("Trade_Date", "delivery_date", "delivery_hour", 
                "delivery_interval"))
        smp.part <- inner_join(smp.part, dataset.smp.output, 
            by = c("Trade_Date", "delivery_date", "delivery_hour", 
                "delivery_interval"))
        
        smp.part$Trade_Date <- smp.part$Trade_Date - 1
        smp.part$delivery_date <- smp.part$delivery_date + 1
        colnames(smp.part) <- c("Report_Date", "Delivery_Date", 
            "Delivery_Hour", "Delivery_Interval", "SMP_D_Euro", 
            "SMP_D_Minus_6_Euro", "SMP_D_Minus_13_Euro", "SMP_HH_Minus_1_Euro", 
            "SMP_HH_Minus_2_Euro", "Output_SMP_Euro")
        
        sqlquery.loadDemand <- paste("SELECT [REPORT_DATE],[TRADE_DATE],[JURISDICTION],[DELIVERY_DATE],[DELIVERY_HOUR],[DELIVERY_INTERVAL],[FORECAST_MW] FROM [dbo].[WA_SEMO_FourDayLoadForecast] where [report_date] between '", 
            start, "' and '", end, "' order by report_date asc, delivery_hour asc, delivery_interval asc")
        load.part <- sqlQuery(conn, sqlquery.loadDemand)
        
        load.part <- load.part %>% arrange(REPORT_DATE)
        load.part$REPORT_DATE <- as.Date(as.character(load.part$REPORT_DATE))
        load.part <- load.part %>% group_by(REPORT_DATE, DELIVERY_DATE, 
            DELIVERY_HOUR, DELIVERY_INTERVAL, JURISDICTION) %>% 
            summarise(loadDemand = first(FORECAST_MW))
        load.part <- load.part %>% group_by(REPORT_DATE, DELIVERY_DATE, 
            DELIVERY_HOUR, DELIVERY_INTERVAL) %>% summarise(loadDemand = sum(loadDemand))
        
        load.part$DELIVERY_DATE <- as.Date(as.character(load.part$DELIVERY_DATE))
        colnames(load.part) <- c("Report_Date", "Delivery_Date", 
            "Delivery_Hour", "Delivery_Interval", "LoadDemand")
        
        sqlquery.power <- paste("SELECT [REPORT_DATE],[DELIVERY_DATE],[REGIONS],[SOLARPOWER_MW],[WINDPOWER_MW] FROM [dbo].[WA_Metagroup_RenewablePowerProduction] where [report_date] between '", 
            start, "' and '", end, "' order by report_date asc")
        power.part <- sqlQuery(conn, sqlquery.power)
        
        power.part <- power.part %>% mutate(Power = SOLARPOWER_MW + 
            WINDPOWER_MW) %>% arrange(REPORT_DATE, DELIVERY_DATE, 
            REGIONS)
        power.part$REPORT_DATE <- as.Date(as.character(power.part$REPORT_DATE))
        
        power.part.ireland <- power.part %>% filter(REGIONS != 
            "UK")
        power.part.ireland <- power.part.ireland %>% group_by(REPORT_DATE, 
            DELIVERY_DATE) %>% summarise(Power = sum(Power)) %>% 
            separate(DELIVERY_DATE, c("DELIVERY_DATE", "DELIVERY_HOUR"), 
                sep = " ")
        power.part.ireland$DELIVERY_HOUR <- format(strptime(power.part.ireland$DELIVERY_HOUR, 
            format = "%H:%M:%S"), format = "%H")
        power.part.ireland$DELIVERY_HOUR <- as.numeric(power.part.ireland$DELIVERY_HOUR) + 
            1
        power.part.ireland$DELIVERY_DATE <- as.Date(as.character(power.part.ireland$DELIVERY_DATE))
        colnames(power.part.ireland) <- c("Report_Date", "Delivery_Date", 
            "Delivery_Hour", "Power_Production_Ireland")
        
        power.part.UK <- power.part %>% filter(REGIONS == "UK")
        power.part.UK <- power.part.UK %>% group_by(REPORT_DATE, 
            DELIVERY_DATE) %>% summarise(Power = sum(Power)) %>% 
            separate(DELIVERY_DATE, c("DELIVERY_DATE", "DELIVERY_HOUR"), 
                sep = " ")
        power.part.UK$DELIVERY_HOUR <- format(strptime(power.part.UK$DELIVERY_HOUR, 
            format = "%H:%M:%S"), format = "%H")
        power.part.UK$DELIVERY_HOUR <- as.numeric(power.part.UK$DELIVERY_HOUR) + 
            1
        power.part.UK$DELIVERY_DATE <- as.Date(as.character(power.part.UK$DELIVERY_DATE))
        colnames(power.part.UK) <- c("Report_Date", "Delivery_Date", 
            "Delivery_Hour", "Power_Production_UK")
        
        dataset.temp <- inner_join(smp.part, load.part, by = c("Report_Date", 
            "Delivery_Date", "Delivery_Hour", "Delivery_Interval"))
        dataset.temp <- inner_join(dataset.temp, power.part.ireland, 
            by = c("Report_Date", "Delivery_Date", "Delivery_Hour"))
        dataset.temp <- inner_join(dataset.temp, power.part.UK, 
            by = c("Report_Date", "Delivery_Date", "Delivery_Hour"))
        
            dataset.temp <- data.frame(Report_Date = dataset.temp$Report_Date, Delivery_Date = dataset.temp$Delivery_Date, 
                Delivery_Hour = dataset.temp$Delivery_Hour, Delivery_Interval = dataset.temp$Delivery_Interval, 
                SMP_D_Euro = dataset.temp$SMP_D_Euro, SMP_D_Minus_6_Euro = dataset.temp$SMP_D_Minus_6_Euro, 
                SMP_D_Minus_13_Euro = dataset.temp$SMP_D_Minus_13_Euro, 
                SMP_HH_Minus_1_Euro = dataset.temp$SMP_HH_Minus_1_Euro, 
                SMP_HH_Minus_2_Euro = dataset.temp$SMP_HH_Minus_2_Euro, 
                LoadDemand = dataset.temp$LoadDemand, Power_Production_Ireland = dataset.temp$Power_Production_Ireland, 
                Power_Production_UK = dataset.temp$Power_Production_UK, 
                Output_SMP_Euro = dataset.temp$Output_SMP_Euro)
    
    varnames <- colnames(dataset.train)
    jsonVar <- serializeJSON(varnames)
    
    method <- list(Name = "None", Description = "Do Nothing")
    
    input <- list(SQL_Query = jsonVar, Training_Testing_Rate = rate, 
        Input_Discription = Input_Discription, StartDate_Training = start.train, 
        EndDate_Training = end.train, StartDate_Testing = start.test, 
        EndDate_Testing = end.test)
    
    Input_Discription <- "Dataset of Weekly Value with UK Power Production: Dhh, D-7hh, D-14hh, Dhh-1, Dhh-2 with UK"
    
    return(list(dataset.train = dataset.train, dataset.test = dataset.test, 
        input = input, preprocessing = method, Input_Discription = Input_Discription))
}

DatasetAquisition.WeeklyOfDayPlusHalfHourWithoutUK <- function(conn, 
    start.train = (Sys.Date() - 15), end.train = Sys.Date(), 
    start.test = (Sys.Date() - 15), end.test = Sys.Date()) {
    
    for (i in 1:2) {
        
        if (i == 1) {
            start <- start.train
            end <- end.train
        } else {
            start <- start.test
            end <- end.test
        }
        sqlquery <- paste("SELECT [Trade_Date],[delivery_date],[delivery_hour],[delivery_interval],[SMP] FROM [dbo].[WA_SEMO_MarketResults] where [delivery_date] between '", 
            (start - 20), "' and '", (end + 1), "' order by delivery_date asc, delivery_hour asc, delivery_interval asc")
        dataset.smp <- sqlQuery(conn, sqlquery)
        dataset.smp[dataset.smp < 0] <- 0
        dataset.smp$delivery_date <- as.Date(as.character(dataset.smp$delivery_date))
        dataset.smp$Trade_Date <- as.Date(as.character(dataset.smp$Trade_Date))
        
        dataset.smp.lag7 <- dataset.smp
        dataset.smp.lag7$delivery_date <- dataset.smp$delivery_date + 
            6
        dataset.smp.lag7$Trade_Date <- dataset.smp$Trade_Date + 
            6
        
        dataset.smp.lag14 <- dataset.smp
        dataset.smp.lag14$delivery_date <- dataset.smp$delivery_date + 
            13
        dataset.smp.lag14$Trade_Date <- dataset.smp$Trade_Date + 
            13
        
        dataset.smp.hhlag1 <- dataset.smp[, c("Trade_Date", "delivery_date", 
            "delivery_hour", "delivery_interval")]
        dataset.smp.hhlag1$SMP_HH_Minus_1_Euro <- c(NA, dataset.smp$SMP[1:(nrow(dataset.smp) - 
            1)])
        
        dataset.smp.hhlag2 <- dataset.smp[, c("Trade_Date", "delivery_date", 
            "delivery_hour", "delivery_interval")]
        dataset.smp.hhlag2$SMP_HH_Minus_2_Euro <- c(NA, NA, dataset.smp$SMP[1:(nrow(dataset.smp) - 
            2)])
        
        dataset.smp.output <- dataset.smp
        dataset.smp.output$delivery_date <- dataset.smp$delivery_date - 
            1
        dataset.smp.output$Trade_Date <- dataset.smp$Trade_Date - 
            1
        
        smp.part <- inner_join(dataset.smp, dataset.smp.lag7, 
            by = c("Trade_Date", "delivery_date", "delivery_hour", 
                "delivery_interval"))
        smp.part <- inner_join(smp.part, dataset.smp.lag14, by = c("Trade_Date", 
            "delivery_date", "delivery_hour", "delivery_interval"))
        smp.part <- inner_join(smp.part, dataset.smp.hhlag1, 
            by = c("Trade_Date", "delivery_date", "delivery_hour", 
                "delivery_interval"))
        smp.part <- inner_join(smp.part, dataset.smp.hhlag2, 
            by = c("Trade_Date", "delivery_date", "delivery_hour", 
                "delivery_interval"))
        smp.part <- inner_join(smp.part, dataset.smp.output, 
            by = c("Trade_Date", "delivery_date", "delivery_hour", 
                "delivery_interval"))
        
        smp.part$Trade_Date <- smp.part$Trade_Date - 1
        smp.part$delivery_date <- smp.part$delivery_date + 1
        colnames(smp.part) <- c("Report_Date", "Delivery_Date", 
            "Delivery_Hour", "Delivery_Interval", "SMP_D_Euro", 
            "SMP_D_Minus_6_Euro", "SMP_D_Minus_13_Euro", "SMP_HH_Minus_1_Euro", 
            "SMP_HH_Minus_2_Euro", "Output_SMP_Euro")
        
        sqlquery.loadDemand <- paste("SELECT [REPORT_DATE],[TRADE_DATE],[JURISDICTION],[DELIVERY_DATE],[DELIVERY_HOUR],[DELIVERY_INTERVAL],[FORECAST_MW] FROM [dbo].[WA_SEMO_FourDayLoadForecast] where [report_date] between '", 
            start, "' and '", end, "' order by report_date asc, delivery_hour asc, delivery_interval asc")
        load.part <- sqlQuery(conn, sqlquery.loadDemand)
        
        load.part <- load.part %>% arrange(REPORT_DATE)
        load.part$REPORT_DATE <- as.Date(as.character(load.part$REPORT_DATE))
        load.part <- load.part %>% group_by(REPORT_DATE, DELIVERY_DATE, 
            DELIVERY_HOUR, DELIVERY_INTERVAL, JURISDICTION) %>% 
            summarise(loadDemand = first(FORECAST_MW))
        load.part <- load.part %>% group_by(REPORT_DATE, DELIVERY_DATE, 
            DELIVERY_HOUR, DELIVERY_INTERVAL) %>% summarise(loadDemand = sum(loadDemand))
        
        load.part$DELIVERY_DATE <- as.Date(as.character(load.part$DELIVERY_DATE))
        colnames(load.part) <- c("Report_Date", "Delivery_Date", 
            "Delivery_Hour", "Delivery_Interval", "LoadDemand")
        
        sqlquery.power <- paste("SELECT [REPORT_DATE],[DELIVERY_DATE],[REGIONS],[SOLARPOWER_MW],[WINDPOWER_MW] FROM [dbo].[WA_Metagroup_RenewablePowerProduction] where [report_date] between '", 
            start, "' and '", end, "' order by report_date asc")
        power.part <- sqlQuery(conn, sqlquery.power)
        
        power.part <- power.part %>% mutate(Power = SOLARPOWER_MW + 
            WINDPOWER_MW) %>% arrange(REPORT_DATE, DELIVERY_DATE, 
            REGIONS)
        power.part$REPORT_DATE <- as.Date(as.character(power.part$REPORT_DATE))
        
        power.part.ireland <- power.part %>% filter(REGIONS != 
            "UK")
        power.part.ireland <- power.part.ireland %>% group_by(REPORT_DATE, 
            DELIVERY_DATE) %>% summarise(Power = sum(Power)) %>% 
            separate(DELIVERY_DATE, c("DELIVERY_DATE", "DELIVERY_HOUR"), 
                sep = " ")
        power.part.ireland$DELIVERY_HOUR <- format(strptime(power.part.ireland$DELIVERY_HOUR, 
            format = "%H:%M:%S"), format = "%H")
        power.part.ireland$DELIVERY_HOUR <- as.numeric(power.part.ireland$DELIVERY_HOUR) + 
            1
        power.part.ireland$DELIVERY_DATE <- as.Date(as.character(power.part.ireland$DELIVERY_DATE))
        colnames(power.part.ireland) <- c("Report_Date", "Delivery_Date", 
            "Delivery_Hour", "Power_Production_Ireland")
        
        dataset.temp <- inner_join(smp.part, load.part, by = c("Report_Date", 
            "Delivery_Date", "Delivery_Hour", "Delivery_Interval"))
        dataset.temp <- inner_join(dataset.temp, power.part.ireland, 
            by = c("Report_Date", "Delivery_Date", "Delivery_Hour"))
        
        if (i == 1) {
            dataset.train <- data.frame(Delivery_Date = dataset.temp$Delivery_Date, 
                Delivery_Hour = dataset.temp$Delivery_Hour, Delivery_Interval = dataset.temp$Delivery_Interval, 
                SMP_D_Euro = dataset.temp$SMP_D_Euro, SMP_D_Minus_6_Euro = dataset.temp$SMP_D_Minus_6_Euro, 
                SMP_D_Minus_13_Euro = dataset.temp$SMP_D_Minus_13_Euro, 
                SMP_HH_Minus_1_Euro = dataset.temp$SMP_HH_Minus_1_Euro, 
                SMP_HH_Minus_2_Euro = dataset.temp$SMP_HH_Minus_2_Euro, 
                LoadDemand = dataset.temp$LoadDemand, Power_Production_Ireland = dataset.temp$Power_Production_Ireland, 
                Output_SMP_Euro = dataset.temp$Output_SMP_Euro)
        } else {
            dataset.test <- data.frame(Delivery_Date = dataset.temp$Delivery_Date, 
                Delivery_Hour = dataset.temp$Delivery_Hour, Delivery_Interval = dataset.temp$Delivery_Interval, 
                SMP_D_Euro = dataset.temp$SMP_D_Euro, SMP_D_Minus_6_Euro = dataset.temp$SMP_D_Minus_6_Euro, 
                SMP_D_Minus_13_Euro = dataset.temp$SMP_D_Minus_13_Euro, 
                SMP_HH_Minus_1_Euro = dataset.temp$SMP_HH_Minus_1_Euro, 
                SMP_HH_Minus_2_Euro = dataset.temp$SMP_HH_Minus_2_Euro, 
                LoadDemand = dataset.temp$LoadDemand, Power_Production_Ireland = dataset.temp$Power_Production_Ireland, 
                Output_SMP_Euro = dataset.temp$Output_SMP_Euro)
        }
    }
    
    varnames <- colnames(dataset.train)
    jsonVar <- serializeJSON(varnames)
    
    method <- list(Name = "None", Description = "Do Nothing")
    
    input <- list(SQL_Query = jsonVar, Training_Testing_Rate = rate, 
        Input_Discription = Input_Discription, StartDate_Training = start.train, 
        EndDate_Training = end.train, StartDate_Testing = start.test, 
        EndDate_Testing = end.test)
    
    Input_Discription <- "Dataset of Weekly Value without UK Power Production: Dhh, D-7hh, D-14hh, Dhh-1, Dhh-2 without UK"
    
    return(list(dataset.train = dataset.train, dataset.test = dataset.test, 
        input = input, preprocessing = method, Input_Discription = Input_Discription))
}

DatasetAquisition.LagWeekWithoutUK <- function(conn, start.train = (Sys.Date() - 
    15), end.train = Sys.Date(), start.test = (Sys.Date() - 15), 
    end.test = Sys.Date()) {
    
    for (i in 1:2) {
        
        if (i == 1) {
            start <- start.train
            end <- end.train
        } else {
            start <- start.test
            end <- end.test
        }
        sqlquery <- paste("SELECT [Trade_Date],[delivery_date],[delivery_hour],[delivery_interval],[SMP] FROM [dbo].[WA_SEMO_MarketResults] where [delivery_date] between '", 
            (start - 20), "' and '", (end + 1), "' order by delivery_date asc, delivery_hour asc, delivery_interval asc")
        dataset.smp <- sqlQuery(conn, sqlquery)
        dataset.smp[dataset.smp < 0] <- 0
        dataset.smp$delivery_date <- as.Date(as.character(dataset.smp$delivery_date))
        dataset.smp$Trade_Date <- as.Date(as.character(dataset.smp$Trade_Date))
        
        dataset.smp.lag7 <- dataset.smp
        dataset.smp.lag7$delivery_date <- dataset.smp$delivery_date + 
            6
        dataset.smp.lag7$Trade_Date <- dataset.smp$Trade_Date + 
            6
        
        dataset.smp.output <- dataset.smp
        dataset.smp.output$delivery_date <- dataset.smp$delivery_date - 
            1
        dataset.smp.output$Trade_Date <- dataset.smp$Trade_Date - 
            1
        
        smp.part <- inner_join(dataset.smp, dataset.smp.lag7, 
            by = c("Trade_Date", "delivery_date", "delivery_hour", 
                "delivery_interval"))
        smp.part <- inner_join(smp.part, dataset.smp.output, 
            by = c("Trade_Date", "delivery_date", "delivery_hour", 
                "delivery_interval"))
        smp.part$Trade_Date <- smp.part$Trade_Date - 1
        smp.part$delivery_date <- smp.part$delivery_date + 1
        colnames(smp.part) <- c("Report_Date", "Delivery_Date", 
            "Delivery_Hour", "Delivery_Interval", "SMP_D_Euro", 
            "SMP_D_Minus_6_Euro", "Output_SMP_Euro")
        
        sqlquery.loadDemand <- paste("SELECT [REPORT_DATE],[TRADE_DATE],[JURISDICTION],[DELIVERY_DATE],[DELIVERY_HOUR],[DELIVERY_INTERVAL],[FORECAST_MW] FROM [dbo].[WA_SEMO_FourDayLoadForecast] where [report_date] between '", 
            start, "' and '", end, "' order by report_date asc, delivery_hour asc, delivery_interval asc")
        load.part <- sqlQuery(conn, sqlquery.loadDemand)
        
        load.part <- load.part %>% arrange(REPORT_DATE)
        load.part$REPORT_DATE <- as.Date(as.character(load.part$REPORT_DATE))
        load.part <- load.part %>% group_by(REPORT_DATE, DELIVERY_DATE, 
            DELIVERY_HOUR, DELIVERY_INTERVAL, JURISDICTION) %>% 
            summarise(loadDemand = first(FORECAST_MW))
        load.part <- load.part %>% group_by(REPORT_DATE, DELIVERY_DATE, 
            DELIVERY_HOUR, DELIVERY_INTERVAL) %>% summarise(loadDemand = sum(loadDemand))
        
        load.part$DELIVERY_DATE <- as.Date(as.character(load.part$DELIVERY_DATE))
        colnames(load.part) <- c("Report_Date", "Delivery_Date", 
            "Delivery_Hour", "Delivery_Interval", "LoadDemand")
        
        sqlquery.power <- paste("SELECT [REPORT_DATE],[DELIVERY_DATE],[REGIONS],[SOLARPOWER_MW],[WINDPOWER_MW] FROM [dbo].[WA_Metagroup_RenewablePowerProduction] where [report_date] between '", 
            start, "' and '", end, "' order by report_date asc")
        power.part <- sqlQuery(conn, sqlquery.power)
        
        power.part <- power.part %>% mutate(Power = SOLARPOWER_MW + 
            WINDPOWER_MW) %>% arrange(REPORT_DATE, DELIVERY_DATE, 
            REGIONS)
        power.part$REPORT_DATE <- as.Date(as.character(power.part$REPORT_DATE))
        
        power.part.ireland <- power.part %>% filter(REGIONS != 
            "UK")
        power.part.ireland <- power.part.ireland %>% group_by(REPORT_DATE, 
            DELIVERY_DATE) %>% summarise(Power = sum(Power)) %>% 
            separate(DELIVERY_DATE, c("DELIVERY_DATE", "DELIVERY_HOUR"), 
                sep = " ")
        power.part.ireland$DELIVERY_HOUR <- format(strptime(power.part.ireland$DELIVERY_HOUR, 
            format = "%H:%M:%S"), format = "%H")
        power.part.ireland$DELIVERY_HOUR <- as.numeric(power.part.ireland$DELIVERY_HOUR) + 
            1
        power.part.ireland$DELIVERY_DATE <- as.Date(as.character(power.part.ireland$DELIVERY_DATE))
        colnames(power.part.ireland) <- c("Report_Date", "Delivery_Date", 
            "Delivery_Hour", "Power_Production_Ireland")
        
        dataset.temp <- inner_join(smp.part, load.part, by = c("Report_Date", 
            "Delivery_Date", "Delivery_Hour", "Delivery_Interval"))
        dataset.temp <- inner_join(dataset.temp, power.part.ireland, 
            by = c("Report_Date", "Delivery_Date", "Delivery_Hour"))
        
        if (i == 1) {
            dataset.train <- data.frame(Delivery_Date = dataset.temp$Delivery_Date, 
                Delivery_Hour = dataset.temp$Delivery_Hour, Delivery_Interval = dataset.temp$Delivery_Interval, 
                SMP_D_Euro = dataset.temp$SMP_D_Euro, SMP_D_Minus_6_Euro = dataset.temp$SMP_D_Minus_6_Euro, 
                LoadDemand = dataset.temp$LoadDemand, Power_Production_Ireland = dataset.temp$Power_Production_Ireland, 
                Output_SMP_Euro = dataset.temp$Output_SMP_Euro)
        } else {
            dataset.test <- data.frame(Delivery_Date = dataset.temp$Delivery_Date, 
                Delivery_Hour = dataset.temp$Delivery_Hour, Delivery_Interval = dataset.temp$Delivery_Interval, 
                SMP_D_Euro = dataset.temp$SMP_D_Euro, SMP_D_Minus_6_Euro = dataset.temp$SMP_D_Minus_6_Euro, 
                LoadDemand = dataset.temp$LoadDemand, Power_Production_Ireland = dataset.temp$Power_Production_Ireland, 
                Output_SMP_Euro = dataset.temp$Output_SMP_Euro)
        }
    }
    
    varnames <- colnames(dataset.train)
    jsonVar <- serializeJSON(varnames)
    
    method <- list(Name = "None", Description = "Do Nothing")
    
    input <- list(SQL_Query = jsonVar, Training_Testing_Rate = rate, 
        Input_Discription = Input_Discription, StartDate_Training = start.train, 
        EndDate_Training = end.train, StartDate_Testing = start.test, 
        EndDate_Testing = end.test)
    
    Input_Discription <- "Dataset of Weekly Value without UK Power Production: Dhh, D-7hh without UK"
    return(list(dataset.train = dataset.train, dataset.test = dataset.test, 
        input = input, preprocessing = method, Input_Discription = Input_Discription))
}

DatasetAquisition.LagWeekWithUK <- function(conn, start.train = (Sys.Date() - 
    15), end.train = Sys.Date(), start.test = (Sys.Date() - 15), 
    end.test = Sys.Date()) {
    
    for (i in 1:2) {
        
        if (i == 1) {
            start <- start.train
            end <- end.train
        } else {
            start <- start.test
            end <- end.test
        }
        sqlquery <- paste("SELECT [Trade_Date],[delivery_date],[delivery_hour],[delivery_interval],[SMP] FROM [dbo].[WA_SEMO_MarketResults] where [delivery_date] between '", 
            (start - 20), "' and '", (end + 1), "' order by delivery_date asc, delivery_hour asc, delivery_interval asc")
        dataset.smp <- sqlQuery(conn, sqlquery)
        dataset.smp[dataset.smp < 0] <- 0
        dataset.smp$delivery_date <- as.Date(as.character(dataset.smp$delivery_date))
        dataset.smp$Trade_Date <- as.Date(as.character(dataset.smp$Trade_Date))
        
        dataset.smp.lag7 <- dataset.smp
        dataset.smp.lag7$delivery_date <- dataset.smp$delivery_date + 
            6
        dataset.smp.lag7$Trade_Date <- dataset.smp$Trade_Date + 
            6
        
        dataset.smp.output <- dataset.smp
        dataset.smp.output$delivery_date <- dataset.smp$delivery_date - 
            1
        dataset.smp.output$Trade_Date <- dataset.smp$Trade_Date - 
            1
        
        smp.part <- inner_join(dataset.smp, dataset.smp.lag7, 
            by = c("Trade_Date", "delivery_date", "delivery_hour", 
                "delivery_interval"))
        smp.part <- inner_join(smp.part, dataset.smp.output, 
            by = c("Trade_Date", "delivery_date", "delivery_hour", 
                "delivery_interval"))
        smp.part$Trade_Date <- smp.part$Trade_Date - 1
        smp.part$delivery_date <- smp.part$delivery_date + 1
        colnames(smp.part) <- c("Report_Date", "Delivery_Date", 
            "Delivery_Hour", "Delivery_Interval", "SMP_D_Euro", 
            "SMP_D_Minus_6_Euro", "Output_SMP_Euro")
        
        sqlquery.loadDemand <- paste("SELECT [REPORT_DATE],[TRADE_DATE],[JURISDICTION],[DELIVERY_DATE],[DELIVERY_HOUR],[DELIVERY_INTERVAL],[FORECAST_MW] FROM [dbo].[WA_SEMO_FourDayLoadForecast] where [report_date] between '", 
            start, "' and '", end, "' order by report_date asc, delivery_hour asc, delivery_interval asc")
        load.part <- sqlQuery(conn, sqlquery.loadDemand)
        
        load.part <- load.part %>% arrange(REPORT_DATE)
        load.part$REPORT_DATE <- as.Date(as.character(load.part$REPORT_DATE))
        load.part <- load.part %>% group_by(REPORT_DATE, DELIVERY_DATE, 
            DELIVERY_HOUR, DELIVERY_INTERVAL, JURISDICTION) %>% 
            summarise(loadDemand = first(FORECAST_MW))
        load.part <- load.part %>% group_by(REPORT_DATE, DELIVERY_DATE, 
            DELIVERY_HOUR, DELIVERY_INTERVAL) %>% summarise(loadDemand = sum(loadDemand))
        
        load.part$DELIVERY_DATE <- as.Date(as.character(load.part$DELIVERY_DATE))
        colnames(load.part) <- c("Report_Date", "Delivery_Date", 
            "Delivery_Hour", "Delivery_Interval", "LoadDemand")
        
        sqlquery.power <- paste("SELECT [REPORT_DATE],[DELIVERY_DATE],[REGIONS],[SOLARPOWER_MW],[WINDPOWER_MW] FROM [dbo].[WA_Metagroup_RenewablePowerProduction] where [report_date] between '", 
            start, "' and '", end, "' order by report_date asc")
        power.part <- sqlQuery(conn, sqlquery.power)
        
        power.part <- power.part %>% mutate(Power = SOLARPOWER_MW + 
            WINDPOWER_MW) %>% arrange(REPORT_DATE, DELIVERY_DATE, 
            REGIONS)
        power.part$REPORT_DATE <- as.Date(as.character(power.part$REPORT_DATE))
        
        power.part.ireland <- power.part %>% filter(REGIONS != 
            "UK")
        power.part.ireland <- power.part.ireland %>% group_by(REPORT_DATE, 
            DELIVERY_DATE) %>% summarise(Power = sum(Power)) %>% 
            separate(DELIVERY_DATE, c("DELIVERY_DATE", "DELIVERY_HOUR"), 
                sep = " ")
        power.part.ireland$DELIVERY_HOUR <- format(strptime(power.part.ireland$DELIVERY_HOUR, 
            format = "%H:%M:%S"), format = "%H")
        power.part.ireland$DELIVERY_HOUR <- as.numeric(power.part.ireland$DELIVERY_HOUR) + 
            1
        power.part.ireland$DELIVERY_DATE <- as.Date(as.character(power.part.ireland$DELIVERY_DATE))
        colnames(power.part.ireland) <- c("Report_Date", "Delivery_Date", 
            "Delivery_Hour", "Power_Production_Ireland")
        
        power.part.UK <- power.part %>% filter(REGIONS == "UK")
        power.part.UK <- power.part.UK %>% group_by(REPORT_DATE, 
            DELIVERY_DATE) %>% summarise(Power = sum(Power)) %>% 
            separate(DELIVERY_DATE, c("DELIVERY_DATE", "DELIVERY_HOUR"), 
                sep = " ")
        power.part.UK$DELIVERY_HOUR <- format(strptime(power.part.UK$DELIVERY_HOUR, 
            format = "%H:%M:%S"), format = "%H")
        power.part.UK$DELIVERY_HOUR <- as.numeric(power.part.UK$DELIVERY_HOUR) + 
            1
        power.part.UK$DELIVERY_DATE <- as.Date(as.character(power.part.UK$DELIVERY_DATE))
        colnames(power.part.UK) <- c("Report_Date", "Delivery_Date", 
            "Delivery_Hour", "Power_Production_UK")
        
        dataset.temp <- inner_join(smp.part, load.part, by = c("Report_Date", 
            "Delivery_Date", "Delivery_Hour", "Delivery_Interval"))
        dataset.temp <- inner_join(dataset.temp, power.part.ireland, 
            by = c("Report_Date", "Delivery_Date", "Delivery_Hour"))
        dataset.temp <- inner_join(dataset.temp, power.part.UK, 
            by = c("Report_Date", "Delivery_Date", "Delivery_Hour"))
        
        if (i == 1) {
            dataset.train <- data.frame(Delivery_Date = dataset.temp$Delivery_Date, 
                Delivery_Hour = dataset.temp$Delivery_Hour, Delivery_Interval = dataset.temp$Delivery_Interval, 
                SMP_D_Euro = dataset.temp$SMP_D_Euro, SMP_D_Minus_6_Euro = dataset.temp$SMP_D_Minus_6_Euro, 
                LoadDemand = dataset.temp$LoadDemand, Power_Production_Ireland = dataset.temp$Power_Production_Ireland, 
                Power_Production_UK = dataset.temp$Power_Production_UK, 
                Output_SMP_Euro = dataset.temp$Output_SMP_Euro)
        } else {
            dataset.test <- data.frame(Delivery_Date = dataset.temp$Delivery_Date, 
                Delivery_Hour = dataset.temp$Delivery_Hour, Delivery_Interval = dataset.temp$Delivery_Interval, 
                SMP_D_Euro = dataset.temp$SMP_D_Euro, SMP_D_Minus_6_Euro = dataset.temp$SMP_D_Minus_6_Euro, 
                LoadDemand = dataset.temp$LoadDemand, Power_Production_Ireland = dataset.temp$Power_Production_Ireland, 
                Power_Production_UK = dataset.temp$Power_Production_UK, 
                Output_SMP_Euro = dataset.temp$Output_SMP_Euro)
        }
    }
    
    varnames <- colnames(dataset.train)
    jsonVar <- serializeJSON(varnames)
    
    method <- list(Name = "None", Description = "Do Nothing")
    
    input <- list(SQL_Query = jsonVar, Training_Testing_Rate = rate, 
        Input_Discription = Input_Discription, StartDate_Training = start.train, 
        EndDate_Training = end.train, StartDate_Testing = start.test, 
        EndDate_Testing = end.test)
    
    Input_Discription <- "Dataset of Weekly Value with UK Power Production: Dhh, D-7hh with UK"
    
    return(list(dataset.train = dataset.train, dataset.test = dataset.test, 
        input = input, preprocessing = method, Input_Discription = Input_Discription))
}

DatasetAquisition.LagHHWithoutUK <- function(conn, start.train = (Sys.Date() - 
    15), end.train = Sys.Date(), start.test = (Sys.Date() - 15), 
    end.test = Sys.Date()) {
    
    for (i in 1:2) {
        
        if (i == 1) {
            start <- start.train
            end <- end.train
        } else {
            start <- start.test
            end <- end.test
        }
        sqlquery <- paste("SELECT [Trade_Date],[delivery_date],[delivery_hour],[delivery_interval],[SMP] FROM [dbo].[WA_SEMO_MarketResults] where [delivery_date] between '", 
            (start - 20), "' and '", (end + 1), "' order by delivery_date asc, delivery_hour asc, delivery_interval asc")
        dataset.smp <- sqlQuery(conn, sqlquery)
        dataset.smp[dataset.smp < 0] <- 0
        dataset.smp$delivery_date <- as.Date(as.character(dataset.smp$delivery_date))
        dataset.smp$Trade_Date <- as.Date(as.character(dataset.smp$Trade_Date))
        
        dataset.smp.hhlag1 <- dataset.smp[, c("Trade_Date", "delivery_date", 
            "delivery_hour", "delivery_interval")]
        dataset.smp.hhlag1$SMP_HH_Minus_1_Euro <- c(NA, dataset.smp$SMP[1:(nrow(dataset.smp) - 
            1)])
        
        dataset.smp.hhlag2 <- dataset.smp[, c("Trade_Date", "delivery_date", 
            "delivery_hour", "delivery_interval")]
        dataset.smp.hhlag2$SMP_HH_Minus_2_Euro <- c(NA, NA, dataset.smp$SMP[1:(nrow(dataset.smp) - 
            2)])
        
        dataset.smp.output <- dataset.smp
        dataset.smp.output$delivery_date <- dataset.smp$delivery_date - 
            1
        dataset.smp.output$Trade_Date <- dataset.smp$Trade_Date - 
            1
        
        smp.part <- inner_join(dataset.smp, dataset.smp.hhlag1, 
            by = c("Trade_Date", "delivery_date", "delivery_hour", 
                "delivery_interval"))
        smp.part <- inner_join(smp.part, dataset.smp.hhlag2, 
            by = c("Trade_Date", "delivery_date", "delivery_hour", 
                "delivery_interval"))
        smp.part <- inner_join(smp.part, dataset.smp.output, 
            by = c("Trade_Date", "delivery_date", "delivery_hour", 
                "delivery_interval"))
        smp.part$Trade_Date <- smp.part$Trade_Date - 1
        smp.part$delivery_date <- smp.part$delivery_date + 1
        colnames(smp.part) <- c("Report_Date", "Delivery_Date", 
            "Delivery_Hour", "Delivery_Interval", "SMP_D_Euro", 
            "SMP_HH_Minus_1_Euro", "SMP_HH_Minus_2_Euro", "Output_SMP_Euro")
        
        sqlquery.loadDemand <- paste("SELECT [REPORT_DATE],[TRADE_DATE],[JURISDICTION],[DELIVERY_DATE],[DELIVERY_HOUR],[DELIVERY_INTERVAL],[FORECAST_MW] FROM [dbo].[WA_SEMO_FourDayLoadForecast] where [report_date] between '", 
            start, "' and '", end, "' order by report_date asc, delivery_hour asc, delivery_interval asc")
        load.part <- sqlQuery(conn, sqlquery.loadDemand)
        
        load.part <- load.part %>% arrange(REPORT_DATE)
        load.part$REPORT_DATE <- as.Date(as.character(load.part$REPORT_DATE))
        load.part <- load.part %>% group_by(REPORT_DATE, DELIVERY_DATE, 
            DELIVERY_HOUR, DELIVERY_INTERVAL, JURISDICTION) %>% 
            summarise(loadDemand = first(FORECAST_MW))
        load.part <- load.part %>% group_by(REPORT_DATE, DELIVERY_DATE, 
            DELIVERY_HOUR, DELIVERY_INTERVAL) %>% summarise(loadDemand = sum(loadDemand))
        
        load.part$DELIVERY_DATE <- as.Date(as.character(load.part$DELIVERY_DATE))
        colnames(load.part) <- c("Report_Date", "Delivery_Date", 
            "Delivery_Hour", "Delivery_Interval", "LoadDemand")
        
        sqlquery.power <- paste("SELECT [REPORT_DATE],[DELIVERY_DATE],[REGIONS],[SOLARPOWER_MW],[WINDPOWER_MW] FROM [dbo].[WA_Metagroup_RenewablePowerProduction] where [report_date] between '", 
            start, "' and '", end, "' order by report_date asc")
        power.part <- sqlQuery(conn, sqlquery.power)
        
        power.part <- power.part %>% mutate(Power = SOLARPOWER_MW + 
            WINDPOWER_MW) %>% arrange(REPORT_DATE, DELIVERY_DATE, 
            REGIONS)
        power.part$REPORT_DATE <- as.Date(as.character(power.part$REPORT_DATE))
        
        power.part.ireland <- power.part %>% filter(REGIONS != 
            "UK")
        power.part.ireland <- power.part.ireland %>% group_by(REPORT_DATE, 
            DELIVERY_DATE) %>% summarise(Power = sum(Power)) %>% 
            separate(DELIVERY_DATE, c("DELIVERY_DATE", "DELIVERY_HOUR"), 
                sep = " ")
        power.part.ireland$DELIVERY_HOUR <- format(strptime(power.part.ireland$DELIVERY_HOUR, 
            format = "%H:%M:%S"), format = "%H")
        power.part.ireland$DELIVERY_HOUR <- as.numeric(power.part.ireland$DELIVERY_HOUR) + 
            1
        power.part.ireland$DELIVERY_DATE <- as.Date(as.character(power.part.ireland$DELIVERY_DATE))
        colnames(power.part.ireland) <- c("Report_Date", "Delivery_Date", 
            "Delivery_Hour", "Power_Production_Ireland")
        
        dataset.temp <- inner_join(smp.part, load.part, by = c("Report_Date", 
            "Delivery_Date", "Delivery_Hour", "Delivery_Interval"))
        dataset.temp <- inner_join(dataset.temp, power.part.ireland, 
            by = c("Report_Date", "Delivery_Date", "Delivery_Hour"))
        
        if (i == 1) {
            dataset.train <- data.frame(Delivery_Date = dataset.temp$Delivery_Date, 
                Delivery_Hour = dataset.temp$Delivery_Hour, Delivery_Interval = dataset.temp$Delivery_Interval, 
                SMP_D_Euro = dataset.temp$SMP_D_Euro, SMP_HH_Minus_1_Euro = dataset.temp$SMP_HH_Minus_1_Euro, 
                SMP_HH_Minus_2_Euro = dataset.temp$SMP_HH_Minus_2_Euro, 
                LoadDemand = dataset.temp$LoadDemand, Power_Production_Ireland = dataset.temp$Power_Production_Ireland, 
                Output_SMP_Euro = dataset.temp$Output_SMP_Euro)
        } else {
            dataset.test <- data.frame(Delivery_Date = dataset.temp$Delivery_Date, 
                Delivery_Hour = dataset.temp$Delivery_Hour, Delivery_Interval = dataset.temp$Delivery_Interval, 
                SMP_D_Euro = dataset.temp$SMP_D_Euro, SMP_HH_Minus_1_Euro = dataset.temp$SMP_HH_Minus_1_Euro, 
                SMP_HH_Minus_2_Euro = dataset.temp$SMP_HH_Minus_2_Euro, 
                LoadDemand = dataset.temp$LoadDemand, Power_Production_Ireland = dataset.temp$Power_Production_Ireland, 
                Output_SMP_Euro = dataset.temp$Output_SMP_Euro)
        }
    }
    
    varnames <- colnames(dataset.train)
    jsonVar <- serializeJSON(varnames)
    
    method <- list(Name = "None", Description = "Do Nothing")
    
    input <- list(SQL_Query = jsonVar, Training_Testing_Rate = rate, 
        Input_Discription = Input_Discription, StartDate_Training = start.train, 
        EndDate_Training = end.train, StartDate_Testing = start.test, 
        EndDate_Testing = end.test)
    
    Input_Discription <- "Dataset of Weekly Value without UK Power Production: Dhh, Dhh-1, Dhh-2 without UK"
    
    return(list(dataset.train = dataset.train, dataset.test = dataset.test, 
        input = input, preprocessing = method, Input_Discription = Input_Discription))
}

DatasetAquisition.LagHHWithUK <- function(conn, start.train = (Sys.Date() - 
    15), end.train = Sys.Date(), start.test = (Sys.Date() - 15), 
    end.test = Sys.Date()) {
    
    for (i in 1:2) {
        
        if (i == 1) {
            start <- start.train
            end <- end.train
        } else {
            start <- start.test
            end <- end.test
        }
        sqlquery <- paste("SELECT [Trade_Date],[delivery_date],[delivery_hour],[delivery_interval],[SMP] FROM [dbo].[WA_SEMO_MarketResults] where [delivery_date] between '", 
            (start - 20), "' and '", (end + 1), "' order by delivery_date asc, delivery_hour asc, delivery_interval asc")
        dataset.smp <- sqlQuery(conn, sqlquery)
        dataset.smp[dataset.smp < 0] <- 0
        dataset.smp$delivery_date <- as.Date(as.character(dataset.smp$delivery_date))
        dataset.smp$Trade_Date <- as.Date(as.character(dataset.smp$Trade_Date))
        
        dataset.smp.hhlag1 <- dataset.smp[, c("Trade_Date", "delivery_date", 
            "delivery_hour", "delivery_interval")]
        dataset.smp.hhlag1$SMP_HH_Minus_1_Euro <- c(NA, dataset.smp$SMP[1:(nrow(dataset.smp) - 
            1)])
        
        dataset.smp.hhlag2 <- dataset.smp[, c("Trade_Date", "delivery_date", 
            "delivery_hour", "delivery_interval")]
        dataset.smp.hhlag2$SMP_HH_Minus_2_Euro <- c(NA, NA, dataset.smp$SMP[1:(nrow(dataset.smp) - 
            2)])
        
        dataset.smp.output <- dataset.smp
        dataset.smp.output$delivery_date <- dataset.smp$delivery_date - 
            1
        dataset.smp.output$Trade_Date <- dataset.smp$Trade_Date - 
            1
        
        smp.part <- inner_join(dataset.smp, dataset.smp.hhlag1, 
            by = c("Trade_Date", "delivery_date", "delivery_hour", 
                "delivery_interval"))
        smp.part <- inner_join(smp.part, dataset.smp.hhlag2, 
            by = c("Trade_Date", "delivery_date", "delivery_hour", 
                "delivery_interval"))
        smp.part <- inner_join(smp.part, dataset.smp.output, 
            by = c("Trade_Date", "delivery_date", "delivery_hour", 
                "delivery_interval"))
        smp.part$Trade_Date <- smp.part$Trade_Date - 1
        smp.part$delivery_date <- smp.part$delivery_date + 1
        colnames(smp.part) <- c("Report_Date", "Delivery_Date", 
            "Delivery_Hour", "Delivery_Interval", "SMP_D_Euro", 
            "SMP_HH_Minus_1_Euro", "SMP_HH_Minus_2_Euro", "Output_SMP_Euro")
        
        sqlquery.loadDemand <- paste("SELECT [REPORT_DATE],[TRADE_DATE],[JURISDICTION],[DELIVERY_DATE],[DELIVERY_HOUR],[DELIVERY_INTERVAL],[FORECAST_MW] FROM [dbo].[WA_SEMO_FourDayLoadForecast] where [report_date] between '", 
            start, "' and '", end, "' order by report_date asc, delivery_hour asc, delivery_interval asc")
        load.part <- sqlQuery(conn, sqlquery.loadDemand)
        
        load.part <- load.part %>% arrange(REPORT_DATE)
        load.part$REPORT_DATE <- as.Date(as.character(load.part$REPORT_DATE))
        load.part <- load.part %>% group_by(REPORT_DATE, DELIVERY_DATE, 
            DELIVERY_HOUR, DELIVERY_INTERVAL, JURISDICTION) %>% 
            summarise(loadDemand = first(FORECAST_MW))
        load.part <- load.part %>% group_by(REPORT_DATE, DELIVERY_DATE, 
            DELIVERY_HOUR, DELIVERY_INTERVAL) %>% summarise(loadDemand = sum(loadDemand))
        
        load.part$DELIVERY_DATE <- as.Date(as.character(load.part$DELIVERY_DATE))
        colnames(load.part) <- c("Report_Date", "Delivery_Date", 
            "Delivery_Hour", "Delivery_Interval", "LoadDemand")
        
        sqlquery.power <- paste("SELECT [REPORT_DATE],[DELIVERY_DATE],[REGIONS],[SOLARPOWER_MW],[WINDPOWER_MW] FROM [dbo].[WA_Metagroup_RenewablePowerProduction] where [report_date] between '", 
            start, "' and '", end, "' order by report_date asc")
        power.part <- sqlQuery(conn, sqlquery.power)
        
        power.part <- power.part %>% mutate(Power = SOLARPOWER_MW + 
            WINDPOWER_MW) %>% arrange(REPORT_DATE, DELIVERY_DATE, 
            REGIONS)
        power.part$REPORT_DATE <- as.Date(as.character(power.part$REPORT_DATE))
        
        power.part.ireland <- power.part %>% filter(REGIONS != 
            "UK")
        power.part.ireland <- power.part.ireland %>% group_by(REPORT_DATE, 
            DELIVERY_DATE) %>% summarise(Power = sum(Power)) %>% 
            separate(DELIVERY_DATE, c("DELIVERY_DATE", "DELIVERY_HOUR"), 
                sep = " ")
        power.part.ireland$DELIVERY_HOUR <- format(strptime(power.part.ireland$DELIVERY_HOUR, 
            format = "%H:%M:%S"), format = "%H")
        power.part.ireland$DELIVERY_HOUR <- as.numeric(power.part.ireland$DELIVERY_HOUR) + 
            1
        power.part.ireland$DELIVERY_DATE <- as.Date(as.character(power.part.ireland$DELIVERY_DATE))
        colnames(power.part.ireland) <- c("Report_Date", "Delivery_Date", 
            "Delivery_Hour", "Power_Production_Ireland")
        
        power.part.UK <- power.part %>% filter(REGIONS == "UK")
        power.part.UK <- power.part.UK %>% group_by(REPORT_DATE, 
            DELIVERY_DATE) %>% summarise(Power = sum(Power)) %>% 
            separate(DELIVERY_DATE, c("DELIVERY_DATE", "DELIVERY_HOUR"), 
                sep = " ")
        power.part.UK$DELIVERY_HOUR <- format(strptime(power.part.UK$DELIVERY_HOUR, 
            format = "%H:%M:%S"), format = "%H")
        power.part.UK$DELIVERY_HOUR <- as.numeric(power.part.UK$DELIVERY_HOUR) + 
            1
        power.part.UK$DELIVERY_DATE <- as.Date(as.character(power.part.UK$DELIVERY_DATE))
        colnames(power.part.UK) <- c("Report_Date", "Delivery_Date", 
            "Delivery_Hour", "Power_Production_UK")
        
        dataset.temp <- inner_join(smp.part, load.part, by = c("Report_Date", 
            "Delivery_Date", "Delivery_Hour", "Delivery_Interval"))
        dataset.temp <- inner_join(dataset.temp, power.part.ireland, 
            by = c("Report_Date", "Delivery_Date", "Delivery_Hour"))
        dataset.temp <- inner_join(dataset.temp, power.part.UK, 
            by = c("Report_Date", "Delivery_Date", "Delivery_Hour"))
        
        if (i == 1) {
            dataset.train <- data.frame(Delivery_Date = dataset.temp$Delivery_Date, 
                Delivery_Hour = dataset.temp$Delivery_Hour, Delivery_Interval = dataset.temp$Delivery_Interval, 
                SMP_D_Euro = dataset.temp$SMP_D_Euro, SMP_HH_Minus_1_Euro = dataset.temp$SMP_HH_Minus_1_Euro, 
                SMP_HH_Minus_2_Euro = dataset.temp$SMP_HH_Minus_2_Euro, 
                LoadDemand = dataset.temp$LoadDemand, Power_Production_Ireland = dataset.temp$Power_Production_Ireland, 
                Power_Production_UK = dataset.temp$Power_Production_UK, 
                Output_SMP_Euro = dataset.temp$Output_SMP_Euro)
        } else {
            dataset.test <- data.frame(Delivery_Date = dataset.temp$Delivery_Date, 
                Delivery_Hour = dataset.temp$Delivery_Hour, Delivery_Interval = dataset.temp$Delivery_Interval, 
                SMP_D_Euro = dataset.temp$SMP_D_Euro, SMP_HH_Minus_1_Euro = dataset.temp$SMP_HH_Minus_1_Euro, 
                SMP_HH_Minus_2_Euro = dataset.temp$SMP_HH_Minus_2_Euro, 
                LoadDemand = dataset.temp$LoadDemand, Power_Production_Ireland = dataset.temp$Power_Production_Ireland, 
                Power_Production_UK = dataset.temp$Power_Production_UK, 
                Output_SMP_Euro = dataset.temp$Output_SMP_Euro)
        }
    }
    
    varnames <- colnames(dataset.train)
    jsonVar <- serializeJSON(varnames)
    
    method <- list(Name = "None", Description = "Do Nothing")
    
    input <- list(SQL_Query = jsonVar, Training_Testing_Rate = rate, 
        Input_Discription = Input_Discription, StartDate_Training = start.train, 
        EndDate_Training = end.train, StartDate_Testing = start.test, 
        EndDate_Testing = end.test)
    
    Input_Discription <- "Dataset of Weekly Value with UK Power Production: Dhh, Dhh-1, Dhh-2 with UK"
    return(list(dataset.train = dataset.train, dataset.test = dataset.test, 
        input = input, preprocessing = method, Input_Discription = Input_Discription))
}

DatasetAquisition.PreviousWeekTrendWithoutUK <- function(conn, 
    start.train = (Sys.Date() - 15), end.train = Sys.Date(), 
    start.test = (Sys.Date() - 15), end.test = Sys.Date()) {
    
    start <- min(c(start.train, start.test, end.train, end.test))
    end <- max(c(start.train, start.test, end.train, end.test))
    sqlquery <- paste("SELECT [Trade_Date],[delivery_date],[delivery_hour],[delivery_interval],[SMP] FROM [dbo].[WA_SEMO_MarketResults] where [delivery_date] between '", 
        (start - 20), "' and '", (end + 1), "' order by delivery_date asc, delivery_hour asc, delivery_interval asc")
    dataset.smp <- sqlQuery(conn, sqlquery)
    dataset.smp[dataset.smp < 0] <- 0
    dataset.smp$delivery_date <- as.Date(as.character(dataset.smp$delivery_date))
    dataset.smp$Trade_Date <- as.Date(as.character(dataset.smp$Trade_Date))
    
    dataset.smp.lag7 <- dataset.smp[, c("Trade_Date", "delivery_date", 
        "delivery_hour", "delivery_interval")]
    dataset.smp.lag7$delivery_date <- dataset.smp$delivery_date + 
        6
    dataset.smp.lag7$Trade_Date <- dataset.smp$Trade_Date + 6
    dataset.smp.lag7$SMP_D_Minus_6_Euro <- dataset.smp$SMP
    
    dataset.smp.lag14 <- dataset.smp[, c("Trade_Date", "delivery_date", 
        "delivery_hour", "delivery_interval")]
    dataset.smp.lag14$delivery_date <- dataset.smp$delivery_date + 
        13
    dataset.smp.lag14$Trade_Date <- dataset.smp$Trade_Date + 
        13
    dataset.smp.lag14$SMP_D_Minus_13_Euro <- dataset.smp$SMP
    
    dataset.smp.hhlag1 <- dataset.smp.lag7[, c("Trade_Date", 
        "delivery_date", "delivery_hour", "delivery_interval")]
    dataset.smp.hhlag1$SMP_HH_Minus_1_Euro <- c(NA, dataset.smp.lag7$SMP[1:(nrow(dataset.smp) - 
        1)])
    
    dataset.smp.hhlag2 <- dataset.smp.lag7[, c("Trade_Date", 
        "delivery_date", "delivery_hour", "delivery_interval")]
    dataset.smp.hhlag2$SMP_HH_Minus_2_Euro <- c(NA, NA, dataset.smp.lag7$SMP[1:(nrow(dataset.smp) - 
        2)])
    
    dataset.smp.output <- dataset.smp
    dataset.smp.output$delivery_date <- dataset.smp$delivery_date - 
        1
    dataset.smp.output$Trade_Date <- dataset.smp$Trade_Date - 
        1
    
    smp.part <- inner_join(dataset.smp, dataset.smp.lag7, by = c("Trade_Date", 
        "delivery_date", "delivery_hour", "delivery_interval"))
    smp.part <- inner_join(smp.part, dataset.smp.lag14, by = c("Trade_Date", 
        "delivery_date", "delivery_hour", "delivery_interval"))
    smp.part <- inner_join(smp.part, dataset.smp.hhlag1, by = c("Trade_Date", 
        "delivery_date", "delivery_hour", "delivery_interval"))
    smp.part <- inner_join(smp.part, dataset.smp.hhlag2, by = c("Trade_Date", 
        "delivery_date", "delivery_hour", "delivery_interval"))
    smp.part <- inner_join(smp.part, dataset.smp.output, by = c("Trade_Date", 
        "delivery_date", "delivery_hour", "delivery_interval"))
    
    smp.part$Trade_Date <- smp.part$Trade_Date - 1
    smp.part$delivery_date <- smp.part$delivery_date + 1
    colnames(smp.part) <- c("Report_Date", "Delivery_Date", "Delivery_Hour", 
        "Delivery_Interval", "SMP_D_Euro", "SMP_D_Minus_6_Euro", 
        "SMP_D_Minus_13_Euro", "SMP_HH_Minus_1_Euro", "SMP_HH_Minus_2_Euro", 
        "Output_SMP_Euro")
    
    sqlquery.loadDemand <- paste("SELECT [REPORT_DATE],[TRADE_DATE],[JURISDICTION],[DELIVERY_DATE],[DELIVERY_HOUR],[DELIVERY_INTERVAL],[FORECAST_MW] FROM [dbo].[WA_SEMO_FourDayLoadForecast] where [report_date] between '", 
        start, "' and '", end, "' order by report_date asc, delivery_hour asc, delivery_interval asc")
    load.part <- sqlQuery(conn, sqlquery.loadDemand)
    
    load.part <- load.part %>% arrange(REPORT_DATE)
    load.part$REPORT_DATE <- as.Date(as.character(load.part$REPORT_DATE))
    load.part <- load.part %>% group_by(REPORT_DATE, DELIVERY_DATE, 
        DELIVERY_HOUR, DELIVERY_INTERVAL, JURISDICTION) %>% summarise(loadDemand = first(FORECAST_MW))
    load.part <- load.part %>% group_by(REPORT_DATE, DELIVERY_DATE, 
        DELIVERY_HOUR, DELIVERY_INTERVAL) %>% summarise(loadDemand = sum(loadDemand))
    
    load.part$DELIVERY_DATE <- as.Date(as.character(load.part$DELIVERY_DATE))
    colnames(load.part) <- c("Report_Date", "Delivery_Date", 
        "Delivery_Hour", "Delivery_Interval", "LoadDemand")
    
    sqlquery.power <- paste("SELECT [REPORT_DATE],[DELIVERY_DATE],[REGIONS],[SOLARPOWER_MW],[WINDPOWER_MW] FROM [dbo].[WA_Metagroup_RenewablePowerProduction] where [report_date] between '", 
        start, "' and '", end, "' order by report_date asc")
    power.part <- sqlQuery(conn, sqlquery.power)
    
    power.part <- power.part %>% mutate(Power = SOLARPOWER_MW + 
        WINDPOWER_MW) %>% arrange(REPORT_DATE, DELIVERY_DATE, 
        REGIONS)
    power.part$REPORT_DATE <- as.Date(as.character(power.part$REPORT_DATE))
    
    power.part.ireland <- power.part %>% filter(REGIONS != "UK")
    power.part.ireland <- power.part.ireland %>% group_by(REPORT_DATE, 
        DELIVERY_DATE) %>% summarise(Power = sum(Power)) %>% 
        separate(DELIVERY_DATE, c("DELIVERY_DATE", "DELIVERY_HOUR"), 
            sep = " ")
    power.part.ireland$DELIVERY_HOUR <- format(strptime(power.part.ireland$DELIVERY_HOUR, 
        format = "%H:%M:%S"), format = "%H")
    power.part.ireland$DELIVERY_HOUR <- as.numeric(power.part.ireland$DELIVERY_HOUR) + 
        1
    power.part.ireland$DELIVERY_DATE <- as.Date(as.character(power.part.ireland$DELIVERY_DATE))
    colnames(power.part.ireland) <- c("Report_Date", "Delivery_Date", 
        "Delivery_Hour", "Power_Production_Ireland")
    
    dataset.temp <- inner_join(smp.part, load.part, by = c("Report_Date", 
        "Delivery_Date", "Delivery_Hour", "Delivery_Interval"))
    dataset.temp <- inner_join(dataset.temp, power.part.ireland, 
        by = c("Report_Date", "Delivery_Date", "Delivery_Hour"))
    
    dataset.temp <- data.frame(Report_Date = dataset.temp$Report_Date, 
        Delivery_Date = dataset.temp$Delivery_Date, Delivery_Hour = dataset.temp$Delivery_Hour, 
        Delivery_Interval = dataset.temp$Delivery_Interval, SMP_D_Euro = dataset.temp$SMP_D_Euro, 
        SMP_D_Minus_6_Euro = dataset.temp$SMP_D_Minus_6_Euro, 
        SMP_D_Minus_13_Euro = dataset.temp$SMP_D_Minus_13_Euro, 
        SMP_D_Minus_7_HH_Minus_1_Euro = dataset.temp$SMP_HH_Minus_1_Euro, 
        SMP_D_Minus_7_HH_Minus_2_Euro = dataset.temp$SMP_HH_Minus_2_Euro, 
        LoadDemand = dataset.temp$LoadDemand, Power_Production_Ireland = dataset.temp$Power_Production_Ireland, 
        Output_SMP_Euro = dataset.temp$Output_SMP_Euro)
    
    dataset.train <- dataset.temp %>% filter(Report_Date >= start.train, 
        Report_Date <= end.train) %>% select(-Report_Date)
    dataset.test <- dataset.temp %>% filter(Report_Date >= start.test, 
        Report_Date <= end.test) %>% select(-Report_Date)
    
    varnames <- colnames(dataset.train)
    jsonVar <- serializeJSON(varnames)
    
    method <- list(Name = "None", Description = "Do Nothing")
    
    input <- list(SQL_Query = jsonVar, Training_Testing_Rate = rate, 
        Input_Discription = Input_Discription, StartDate_Training = start.train, 
        EndDate_Training = end.train, StartDate_Testing = start.test, 
        EndDate_Testing = end.test)
    
    Input_Discription <- "Dataset of Weekly Value without UK Power Production: Dhh, D-7hh, D-7hh-1, D-7hh-2 D-14hh without UK"
    return(list(dataset.train = dataset.train, dataset.test = dataset.test, 
        input = input, preprocessing = method, Input_Discription = Input_Discription))
}

DatasetAquisition.PreviousWeekTrendWithUK <- function(conn, 
     start.train = (Sys.Date() - 15), end.train = Sys.Date(), 
     start.test = (Sys.Date() - 15), end.test = Sys.Date()) {
  
  start <- min(c(start.train, start.test, end.train, end.test))
  end <- max(c(start.train, start.test, end.train, end.test))
  sqlquery <- paste("SELECT [Trade_Date],[delivery_date],[delivery_hour],[delivery_interval],[SMP] FROM [dbo].[WA_SEMO_MarketResults] where [delivery_date] between '", 
                    (start - 20), "' and '", (end + 1), "' order by delivery_date asc, delivery_hour asc, delivery_interval asc")
  dataset.smp <- sqlQuery(conn, sqlquery)
  dataset.smp[dataset.smp < 0] <- 0
  dataset.smp$delivery_date <- as.Date(as.character(dataset.smp$delivery_date))
  dataset.smp$Trade_Date <- as.Date(as.character(dataset.smp$Trade_Date))
  
  dataset.smp.lag7 <- dataset.smp[, c("Trade_Date", "delivery_date", 
                                      "delivery_hour", "delivery_interval")]
  dataset.smp.lag7$delivery_date <- dataset.smp$delivery_date + 
    6
  dataset.smp.lag7$Trade_Date <- dataset.smp$Trade_Date + 6
  dataset.smp.lag7$SMP_D_Minus_6_Euro <- dataset.smp$SMP
  
  dataset.smp.lag14 <- dataset.smp[, c("Trade_Date", "delivery_date", 
                                       "delivery_hour", "delivery_interval")]
  dataset.smp.lag14$delivery_date <- dataset.smp$delivery_date + 
    13
  dataset.smp.lag14$Trade_Date <- dataset.smp$Trade_Date + 
    13
  dataset.smp.lag14$SMP_D_Minus_13_Euro <- dataset.smp$SMP
  
  dataset.smp.hhlag1 <- dataset.smp.lag7[, c("Trade_Date", 
                                             "delivery_date", "delivery_hour", "delivery_interval")]
  dataset.smp.hhlag1$SMP_HH_Minus_1_Euro <- c(NA, dataset.smp.lag7$SMP[1:(nrow(dataset.smp) - 
                                                                            1)])
  
  dataset.smp.hhlag2 <- dataset.smp.lag7[, c("Trade_Date", 
                                             "delivery_date", "delivery_hour", "delivery_interval")]
  dataset.smp.hhlag2$SMP_HH_Minus_2_Euro <- c(NA, NA, dataset.smp.lag7$SMP[1:(nrow(dataset.smp) - 
                                                                                2)])
  
  dataset.smp.output <- dataset.smp
  dataset.smp.output$delivery_date <- dataset.smp$delivery_date - 
    1
  dataset.smp.output$Trade_Date <- dataset.smp$Trade_Date - 
    1
  
  smp.part <- inner_join(dataset.smp, dataset.smp.lag7, by = c("Trade_Date", 
                                                               "delivery_date", "delivery_hour", "delivery_interval"))
  smp.part <- inner_join(smp.part, dataset.smp.lag14, by = c("Trade_Date", 
                                                             "delivery_date", "delivery_hour", "delivery_interval"))
  smp.part <- inner_join(smp.part, dataset.smp.hhlag1, by = c("Trade_Date", 
                                                              "delivery_date", "delivery_hour", "delivery_interval"))
  smp.part <- inner_join(smp.part, dataset.smp.hhlag2, by = c("Trade_Date", 
                                                              "delivery_date", "delivery_hour", "delivery_interval"))
  smp.part <- inner_join(smp.part, dataset.smp.output, by = c("Trade_Date", 
                                                              "delivery_date", "delivery_hour", "delivery_interval"))
  
  smp.part$Trade_Date <- smp.part$Trade_Date - 1
  smp.part$delivery_date <- smp.part$delivery_date + 1
  colnames(smp.part) <- c("Report_Date", "Delivery_Date", "Delivery_Hour", 
                          "Delivery_Interval", "SMP_D_Euro", "SMP_D_Minus_6_Euro", 
                          "SMP_D_Minus_13_Euro", "SMP_HH_Minus_1_Euro", "SMP_HH_Minus_2_Euro", 
                          "Output_SMP_Euro")
  
  sqlquery.loadDemand <- paste("SELECT [REPORT_DATE],[TRADE_DATE],[JURISDICTION],[DELIVERY_DATE],[DELIVERY_HOUR],[DELIVERY_INTERVAL],[FORECAST_MW] FROM [dbo].[WA_SEMO_FourDayLoadForecast] where [report_date] between '", 
                               start, "' and '", end, "' order by report_date asc, delivery_hour asc, delivery_interval asc")
  load.part <- sqlQuery(conn, sqlquery.loadDemand)
  
  load.part <- load.part %>% arrange(REPORT_DATE)
  load.part$REPORT_DATE <- as.Date(as.character(load.part$REPORT_DATE))
  load.part <- load.part %>% group_by(REPORT_DATE, DELIVERY_DATE, 
                                      DELIVERY_HOUR, DELIVERY_INTERVAL, JURISDICTION) %>% summarise(loadDemand = first(FORECAST_MW))
  load.part <- load.part %>% group_by(REPORT_DATE, DELIVERY_DATE, 
                                      DELIVERY_HOUR, DELIVERY_INTERVAL) %>% summarise(loadDemand = sum(loadDemand))
  
  load.part$DELIVERY_DATE <- as.Date(as.character(load.part$DELIVERY_DATE))
  colnames(load.part) <- c("Report_Date", "Delivery_Date", 
                           "Delivery_Hour", "Delivery_Interval", "LoadDemand")
  
  sqlquery.power <- paste("SELECT [REPORT_DATE],[DELIVERY_DATE],[REGIONS],[SOLARPOWER_MW],[WINDPOWER_MW] FROM [dbo].[WA_Metagroup_RenewablePowerProduction] where [report_date] between '", 
                          start, "' and '", end, "' order by report_date asc")
  power.part <- sqlQuery(conn, sqlquery.power)
  
  power.part <- power.part %>% mutate(Power = SOLARPOWER_MW + 
                                        WINDPOWER_MW) %>% arrange(REPORT_DATE, DELIVERY_DATE, 
                                                                  REGIONS)
  power.part$REPORT_DATE <- as.Date(as.character(power.part$REPORT_DATE))
  
  power.part.ireland <- power.part %>% filter(REGIONS != "UK")
  power.part.ireland <- power.part.ireland %>% group_by(REPORT_DATE, 
                                                        DELIVERY_DATE) %>% summarise(Power = sum(Power)) %>% 
    separate(DELIVERY_DATE, c("DELIVERY_DATE", "DELIVERY_HOUR"), 
             sep = " ")
  power.part.ireland$DELIVERY_HOUR <- format(strptime(power.part.ireland$DELIVERY_HOUR, 
                                                      format = "%H:%M:%S"), format = "%H")
  power.part.ireland$DELIVERY_HOUR <- as.numeric(power.part.ireland$DELIVERY_HOUR) + 
    1
  power.part.ireland$DELIVERY_DATE <- as.Date(as.character(power.part.ireland$DELIVERY_DATE))
  colnames(power.part.ireland) <- c("Report_Date", "Delivery_Date", 
                                    "Delivery_Hour", "Power_Production_Ireland")
  
  power.part.UK <- power.part %>% filter(REGIONS == "UK")
  power.part.UK <- power.part.UK %>% group_by(REPORT_DATE, 
    DELIVERY_DATE) %>% summarise(Power = sum(Power)) %>% 
    separate(DELIVERY_DATE, c("DELIVERY_DATE", "DELIVERY_HOUR"), 
             sep = " ")
  power.part.UK$DELIVERY_HOUR <- format(strptime(power.part.UK$DELIVERY_HOUR, 
                                                      format = "%H:%M:%S"), format = "%H")
  power.part.UK$DELIVERY_HOUR <- as.numeric(power.part.UK$DELIVERY_HOUR) + 
    1
  power.part.UK$DELIVERY_DATE <- as.Date(as.character(power.part.UK$DELIVERY_DATE))
  colnames(power.part.UK) <- c("Report_Date", "Delivery_Date", 
                                    "Delivery_Hour", "Power_Production_UK")
  
  dataset.temp <- inner_join(smp.part, load.part, by = c("Report_Date", 
                                                         "Delivery_Date", "Delivery_Hour", "Delivery_Interval"))
  dataset.temp <- inner_join(dataset.temp, power.part.ireland, 
                             by = c("Report_Date", "Delivery_Date", "Delivery_Hour"))
  dataset.temp <- inner_join(dataset.temp, power.part.UK, 
                             by = c("Report_Date", "Delivery_Date", "Delivery_Hour"))
  
  dataset.temp <- data.frame(Report_Date = dataset.temp$Report_Date, 
                             Delivery_Date = dataset.temp$Delivery_Date, Delivery_Hour = dataset.temp$Delivery_Hour, 
                             Delivery_Interval = dataset.temp$Delivery_Interval, SMP_D_Euro = dataset.temp$SMP_D_Euro, 
                             SMP_D_Minus_6_Euro = dataset.temp$SMP_D_Minus_6_Euro, 
                             SMP_D_Minus_13_Euro = dataset.temp$SMP_D_Minus_13_Euro, 
                             SMP_D_Minus_7_HH_Minus_1_Euro = dataset.temp$SMP_HH_Minus_1_Euro, 
                             SMP_D_Minus_7_HH_Minus_2_Euro = dataset.temp$SMP_HH_Minus_2_Euro, 
                             LoadDemand = dataset.temp$LoadDemand, Power_Production_Ireland = dataset.temp$Power_Production_Ireland,
                             Power_Production_UK = dataset.temp$Power_Production_UK,
                             Output_SMP_Euro = dataset.temp$Output_SMP_Euro)
  
  dataset.train <- dataset.temp %>% filter(Report_Date >= start.train, 
                                           Report_Date <= end.train) %>% select(-Report_Date)
  dataset.test <- dataset.temp %>% filter(Report_Date >= start.test, 
                                          Report_Date <= end.test) %>% select(-Report_Date)
  
  varnames <- colnames(dataset.train)
  jsonVar <- serializeJSON(varnames)
  
  method <- list(Name = "None", Description = "Do Nothing")
  
  input <- list(SQL_Query = jsonVar, Training_Testing_Rate = rate, 
                Input_Discription = Input_Discription, StartDate_Training = start.train, 
                EndDate_Training = end.train, StartDate_Testing = start.test, 
                EndDate_Testing = end.test)
  
  Input_Discription <- "Dataset of Weekly Value with UK Power Production: Dhh, D-7hh, D-7hh-1, D-7hh-2 D-14hh with UK"
  return(list(dataset.train = dataset.train, dataset.test = dataset.test, 
              input = input, preprocessing = method, Input_Discription = Input_Discription))
}

DatasetAquisition.DifferenceBetweenDemandAndPowerProductionWithoutUK <- function(conn, 
  start.train = (Sys.Date() - 15), end.train = Sys.Date(), 
  start.test = (Sys.Date() - 15), end.test = Sys.Date()) {
  
  start <- min(c(start.train, start.test, end.train, end.test))
  end <- max(c(start.train, start.test, end.train, end.test))
  sqlquery <- paste("SELECT [Trade_Date],[delivery_date],[delivery_hour],[delivery_interval],[SMP] FROM [dbo].[WA_SEMO_MarketResults] where [delivery_date] between '", 
                    (start - 20), "' and '", (end + 1), "' order by delivery_date asc, delivery_hour asc, delivery_interval asc")
  dataset.smp <- sqlQuery(conn, sqlquery)
  dataset.smp[dataset.smp < 0] <- 0
  dataset.smp$delivery_date <- as.Date(as.character(dataset.smp$delivery_date))
  dataset.smp$Trade_Date <- as.Date(as.character(dataset.smp$Trade_Date))
  
  dataset.smp.lag7 <- dataset.smp[, c("Trade_Date", "delivery_date", 
                                      "delivery_hour", "delivery_interval")]
  dataset.smp.lag7$delivery_date <- dataset.smp$delivery_date + 
    6
  dataset.smp.lag7$Trade_Date <- dataset.smp$Trade_Date + 6
  dataset.smp.lag7$SMP_D_Minus_6_Euro <- dataset.smp$SMP
  
  dataset.smp.lag14 <- dataset.smp[, c("Trade_Date", "delivery_date", 
                                       "delivery_hour", "delivery_interval")]
  dataset.smp.lag14$delivery_date <- dataset.smp$delivery_date + 
    13
  dataset.smp.lag14$Trade_Date <- dataset.smp$Trade_Date + 
    13
  dataset.smp.lag14$SMP_D_Minus_13_Euro <- dataset.smp$SMP
  
  dataset.smp.hhlag1 <- dataset.smp.lag7[, c("Trade_Date", 
                                             "delivery_date", "delivery_hour", "delivery_interval")]
  dataset.smp.hhlag1$SMP_HH_Minus_1_Euro <- c(NA, dataset.smp.lag7$SMP[1:(nrow(dataset.smp) - 
                                                                            1)])
  
  dataset.smp.hhlag2 <- dataset.smp.lag7[, c("Trade_Date", 
                                             "delivery_date", "delivery_hour", "delivery_interval")]
  dataset.smp.hhlag2$SMP_HH_Minus_2_Euro <- c(NA, NA, dataset.smp.lag7$SMP[1:(nrow(dataset.smp) - 
                                                                                2)])
  
  dataset.smp.output <- dataset.smp
  dataset.smp.output$delivery_date <- dataset.smp$delivery_date - 
    1
  dataset.smp.output$Trade_Date <- dataset.smp$Trade_Date - 
    1
  
  smp.part <- inner_join(dataset.smp, dataset.smp.lag7, by = c("Trade_Date", 
                                                               "delivery_date", "delivery_hour", "delivery_interval"))
  smp.part <- inner_join(smp.part, dataset.smp.lag14, by = c("Trade_Date", 
                                                             "delivery_date", "delivery_hour", "delivery_interval"))
  smp.part <- inner_join(smp.part, dataset.smp.hhlag1, by = c("Trade_Date", 
                                                              "delivery_date", "delivery_hour", "delivery_interval"))
  smp.part <- inner_join(smp.part, dataset.smp.hhlag2, by = c("Trade_Date", 
                                                              "delivery_date", "delivery_hour", "delivery_interval"))
  smp.part <- inner_join(smp.part, dataset.smp.output, by = c("Trade_Date", 
                                                              "delivery_date", "delivery_hour", "delivery_interval"))
  
  smp.part$Trade_Date <- smp.part$Trade_Date - 1
  smp.part$delivery_date <- smp.part$delivery_date + 1
  colnames(smp.part) <- c("Report_Date", "Delivery_Date", "Delivery_Hour", 
                          "Delivery_Interval", "SMP_D_Euro", "SMP_D_Minus_6_Euro", 
                          "SMP_D_Minus_13_Euro", "SMP_HH_Minus_1_Euro", "SMP_HH_Minus_2_Euro", 
                          "Output_SMP_Euro")
  
  sqlquery.loadDemand <- paste("SELECT [REPORT_DATE],[TRADE_DATE],[JURISDICTION],[DELIVERY_DATE],[DELIVERY_HOUR],[DELIVERY_INTERVAL],[FORECAST_MW] FROM [dbo].[WA_SEMO_FourDayLoadForecast] where [report_date] between '", 
                               start, "' and '", end, "' order by report_date asc, delivery_hour asc, delivery_interval asc")
  load.part <- sqlQuery(conn, sqlquery.loadDemand)
  
  load.part <- load.part %>% arrange(REPORT_DATE)
  load.part$REPORT_DATE <- as.Date(as.character(load.part$REPORT_DATE))
  load.part <- load.part %>% group_by(REPORT_DATE, DELIVERY_DATE, 
                                      DELIVERY_HOUR, DELIVERY_INTERVAL, JURISDICTION) %>% summarise(loadDemand = first(FORECAST_MW))
  load.part <- load.part %>% group_by(REPORT_DATE, DELIVERY_DATE, 
                                      DELIVERY_HOUR, DELIVERY_INTERVAL) %>% summarise(loadDemand = sum(loadDemand))
  
  load.part$DELIVERY_DATE <- as.Date(as.character(load.part$DELIVERY_DATE))
  colnames(load.part) <- c("Report_Date", "Delivery_Date", 
                           "Delivery_Hour", "Delivery_Interval", "LoadDemand")
  
  sqlquery.power <- paste("SELECT [REPORT_DATE],[DELIVERY_DATE],[REGIONS],[SOLARPOWER_MW],[WINDPOWER_MW] FROM [dbo].[WA_Metagroup_RenewablePowerProduction] where [report_date] between '", 
                          start, "' and '", end, "' order by report_date asc")
  power.part <- sqlQuery(conn, sqlquery.power)
  
  power.part <- power.part %>% mutate(Power = SOLARPOWER_MW + 
                                        WINDPOWER_MW) %>% arrange(REPORT_DATE, DELIVERY_DATE, 
                                                                  REGIONS)
  power.part$REPORT_DATE <- as.Date(as.character(power.part$REPORT_DATE))
  
  power.part.ireland <- power.part %>% filter(REGIONS != "UK")
  power.part.ireland <- power.part.ireland %>% group_by(REPORT_DATE, 
                                                        DELIVERY_DATE) %>% summarise(Power = sum(Power)) %>% 
    separate(DELIVERY_DATE, c("DELIVERY_DATE", "DELIVERY_HOUR"), 
             sep = " ")
  power.part.ireland$DELIVERY_HOUR <- format(strptime(power.part.ireland$DELIVERY_HOUR, 
                                                      format = "%H:%M:%S"), format = "%H")
  power.part.ireland$DELIVERY_HOUR <- as.numeric(power.part.ireland$DELIVERY_HOUR) + 
    1
  power.part.ireland$DELIVERY_DATE <- as.Date(as.character(power.part.ireland$DELIVERY_DATE))
  colnames(power.part.ireland) <- c("Report_Date", "Delivery_Date", 
                                    "Delivery_Hour", "Power_Production_Ireland")
  
  dataset.temp <- inner_join(smp.part, load.part, by = c("Report_Date", 
                            "Delivery_Date", "Delivery_Hour", "Delivery_Interval"))
  dataset.temp <- inner_join(dataset.temp, power.part.ireland, 
                             by = c("Report_Date", "Delivery_Date", "Delivery_Hour"))
  
  dataset.temp$difference <- dataset.temp$LoadDemand - dataset.temp$Power_Production_Ireland
  dataset.temp <- data.frame(Report_Date = dataset.temp$Report_Date, 
                             Delivery_Date = dataset.temp$Delivery_Date, Delivery_Hour = dataset.temp$Delivery_Hour, 
                             Delivery_Interval = dataset.temp$Delivery_Interval, SMP_D_Euro = dataset.temp$SMP_D_Euro, 
                             SMP_D_Minus_6_Euro = dataset.temp$SMP_D_Minus_6_Euro, 
                             SMP_D_Minus_13_Euro = dataset.temp$SMP_D_Minus_13_Euro, 
                             SMP_D_Minus_7_HH_Minus_1_Euro = dataset.temp$SMP_HH_Minus_1_Euro, 
                             SMP_D_Minus_7_HH_Minus_2_Euro = dataset.temp$SMP_HH_Minus_2_Euro, 
                             Difference = dataset.temp$difference,
                             Output_SMP_Euro = dataset.temp$Output_SMP_Euro)
  
  dataset.train <- dataset.temp %>% filter(Report_Date >= start.train, 
                                           Report_Date <= end.train) %>% select(-Report_Date)
  dataset.test <- dataset.temp %>% filter(Report_Date >= start.test, 
                                          Report_Date <= end.test) %>% select(-Report_Date)
  
  varnames <- colnames(dataset.train)
  jsonVar <- serializeJSON(varnames)
  
  method <- list(Name = "None", Description = "Do Nothing")
  
  input <- list(SQL_Query = jsonVar, Training_Testing_Rate = rate, 
                Input_Discription = Input_Discription, StartDate_Training = start.train, 
                EndDate_Training = end.train, StartDate_Testing = start.test, 
                EndDate_Testing = end.test)
  
  Input_Discription <- "Dataset of Weekly Value with difference between demand and power production but without UK Power Production: Dhh, D-7hh, D-7hh-1, D-7hh-2 D-14hh with UK"
  return(list(dataset.train = dataset.train, dataset.test = dataset.test, 
              input = input, preprocessing = method, Input_Discription = Input_Discription))
}

DatasetAquisition.DifferenceBetweenDemandAndPowerProductionWithUK <- function(conn, 
   start.train = (Sys.Date() - 15), end.train = Sys.Date(), 
   start.test = (Sys.Date() - 15), end.test = Sys.Date()) {
    
    start <- min(c(start.train, start.test, end.train, end.test))
    end <- max(c(start.train, start.test, end.train, end.test))
    sqlquery <- paste("SELECT [Trade_Date],[delivery_date],[delivery_hour],[delivery_interval],[SMP] FROM [dbo].[WA_SEMO_MarketResults] where [delivery_date] between '", 
                      (start - 20), "' and '", (end + 1), "' order by delivery_date asc, delivery_hour asc, delivery_interval asc")
    dataset.smp <- sqlQuery(conn, sqlquery)
    dataset.smp[dataset.smp < 0] <- 0
    dataset.smp$delivery_date <- as.Date(as.character(dataset.smp$delivery_date))
    dataset.smp$Trade_Date <- as.Date(as.character(dataset.smp$Trade_Date))
    
    dataset.smp.lag7 <- dataset.smp[, c("Trade_Date", "delivery_date", 
                                        "delivery_hour", "delivery_interval")]
    dataset.smp.lag7$delivery_date <- dataset.smp$delivery_date + 
      6
    dataset.smp.lag7$Trade_Date <- dataset.smp$Trade_Date + 6
    dataset.smp.lag7$SMP_D_Minus_6_Euro <- dataset.smp$SMP
    
    dataset.smp.lag14 <- dataset.smp[, c("Trade_Date", "delivery_date", 
                                         "delivery_hour", "delivery_interval")]
    dataset.smp.lag14$delivery_date <- dataset.smp$delivery_date + 
      13
    dataset.smp.lag14$Trade_Date <- dataset.smp$Trade_Date + 
      13
    dataset.smp.lag14$SMP_D_Minus_13_Euro <- dataset.smp$SMP
    
    dataset.smp.hhlag1 <- dataset.smp.lag7[, c("Trade_Date", 
                                               "delivery_date", "delivery_hour", "delivery_interval")]
    dataset.smp.hhlag1$SMP_HH_Minus_1_Euro <- c(NA, dataset.smp.lag7$SMP[1:(nrow(dataset.smp) - 
                                                                              1)])
    
    dataset.smp.hhlag2 <- dataset.smp.lag7[, c("Trade_Date", 
                                               "delivery_date", "delivery_hour", "delivery_interval")]
    dataset.smp.hhlag2$SMP_HH_Minus_2_Euro <- c(NA, NA, dataset.smp.lag7$SMP[1:(nrow(dataset.smp) - 
                                                                                  2)])
    
    dataset.smp.output <- dataset.smp
    dataset.smp.output$delivery_date <- dataset.smp$delivery_date - 
      1
    dataset.smp.output$Trade_Date <- dataset.smp$Trade_Date - 
      1
    
    smp.part <- inner_join(dataset.smp, dataset.smp.lag7, by = c("Trade_Date", 
                                                                 "delivery_date", "delivery_hour", "delivery_interval"))
    smp.part <- inner_join(smp.part, dataset.smp.lag14, by = c("Trade_Date", 
                                                               "delivery_date", "delivery_hour", "delivery_interval"))
    smp.part <- inner_join(smp.part, dataset.smp.hhlag1, by = c("Trade_Date", 
                                                                "delivery_date", "delivery_hour", "delivery_interval"))
    smp.part <- inner_join(smp.part, dataset.smp.hhlag2, by = c("Trade_Date", 
                                                                "delivery_date", "delivery_hour", "delivery_interval"))
    smp.part <- inner_join(smp.part, dataset.smp.output, by = c("Trade_Date", 
                                                                "delivery_date", "delivery_hour", "delivery_interval"))
    
    smp.part$Trade_Date <- smp.part$Trade_Date - 1
    smp.part$delivery_date <- smp.part$delivery_date + 1
    colnames(smp.part) <- c("Report_Date", "Delivery_Date", "Delivery_Hour", 
                            "Delivery_Interval", "SMP_D_Euro", "SMP_D_Minus_6_Euro", 
                            "SMP_D_Minus_13_Euro", "SMP_HH_Minus_1_Euro", "SMP_HH_Minus_2_Euro", 
                            "Output_SMP_Euro")
    
    sqlquery.loadDemand <- paste("SELECT [REPORT_DATE],[TRADE_DATE],[JURISDICTION],[DELIVERY_DATE],[DELIVERY_HOUR],[DELIVERY_INTERVAL],[FORECAST_MW] FROM [dbo].[WA_SEMO_FourDayLoadForecast] where [report_date] between '", 
                                 start, "' and '", end, "' order by report_date asc, delivery_hour asc, delivery_interval asc")
    load.part <- sqlQuery(conn, sqlquery.loadDemand)
    
    load.part <- load.part %>% arrange(REPORT_DATE)
    load.part$REPORT_DATE <- as.Date(as.character(load.part$REPORT_DATE))
    load.part <- load.part %>% group_by(REPORT_DATE, DELIVERY_DATE, 
                                        DELIVERY_HOUR, DELIVERY_INTERVAL, JURISDICTION) %>% summarise(loadDemand = first(FORECAST_MW))
    load.part <- load.part %>% group_by(REPORT_DATE, DELIVERY_DATE, 
                                        DELIVERY_HOUR, DELIVERY_INTERVAL) %>% summarise(loadDemand = sum(loadDemand))
    
    load.part$DELIVERY_DATE <- as.Date(as.character(load.part$DELIVERY_DATE))
    colnames(load.part) <- c("Report_Date", "Delivery_Date", 
                             "Delivery_Hour", "Delivery_Interval", "LoadDemand")
    
    sqlquery.power <- paste("SELECT [REPORT_DATE],[DELIVERY_DATE],[REGIONS],[SOLARPOWER_MW],[WINDPOWER_MW] FROM [dbo].[WA_Metagroup_RenewablePowerProduction] where [report_date] between '", 
                            start, "' and '", end, "' order by report_date asc")
    power.part <- sqlQuery(conn, sqlquery.power)
    
    power.part <- power.part %>% mutate(Power = SOLARPOWER_MW + 
                                          WINDPOWER_MW) %>% arrange(REPORT_DATE, DELIVERY_DATE, 
                                                                    REGIONS)
    power.part$REPORT_DATE <- as.Date(as.character(power.part$REPORT_DATE))
    
    power.part.ireland <- power.part %>% filter(REGIONS != "UK")
    power.part.ireland <- power.part.ireland %>% group_by(REPORT_DATE, 
                                                          DELIVERY_DATE) %>% summarise(Power = sum(Power)) %>% 
      separate(DELIVERY_DATE, c("DELIVERY_DATE", "DELIVERY_HOUR"), 
               sep = " ")
    power.part.ireland$DELIVERY_HOUR <- format(strptime(power.part.ireland$DELIVERY_HOUR, 
                                                        format = "%H:%M:%S"), format = "%H")
    power.part.ireland$DELIVERY_HOUR <- as.numeric(power.part.ireland$DELIVERY_HOUR) + 
      1
    power.part.ireland$DELIVERY_DATE <- as.Date(as.character(power.part.ireland$DELIVERY_DATE))
    colnames(power.part.ireland) <- c("Report_Date", "Delivery_Date", 
                                      "Delivery_Hour", "Power_Production_Ireland")
    
    power.part.UK <- power.part %>% filter(REGIONS == "UK")
    power.part.UK <- power.part.UK %>% group_by(REPORT_DATE, 
                                                DELIVERY_DATE) %>% summarise(Power = sum(Power)) %>% 
      separate(DELIVERY_DATE, c("DELIVERY_DATE", "DELIVERY_HOUR"), 
               sep = " ")
    power.part.UK$DELIVERY_HOUR <- format(strptime(power.part.UK$DELIVERY_HOUR, 
                                                   format = "%H:%M:%S"), format = "%H")
    power.part.UK$DELIVERY_HOUR <- as.numeric(power.part.UK$DELIVERY_HOUR) + 
      1
    power.part.UK$DELIVERY_DATE <- as.Date(as.character(power.part.UK$DELIVERY_DATE))
    colnames(power.part.UK) <- c("Report_Date", "Delivery_Date", 
                                 "Delivery_Hour", "Power_Production_UK")
    
    dataset.temp <- inner_join(smp.part, load.part, by = c("Report_Date", 
                                                           "Delivery_Date", "Delivery_Hour", "Delivery_Interval"))
    dataset.temp <- inner_join(dataset.temp, power.part.ireland, 
                               by = c("Report_Date", "Delivery_Date", "Delivery_Hour"))
    dataset.temp <- inner_join(dataset.temp, power.part.UK, 
                               by = c("Report_Date", "Delivery_Date", "Delivery_Hour"))
    
    dataset.temp$difference <- dataset.temp$LoadDemand - dataset.temp$Power_Production_Ireland
    dataset.temp <- data.frame(Report_Date = dataset.temp$Report_Date, 
                               Delivery_Date = dataset.temp$Delivery_Date, Delivery_Hour = dataset.temp$Delivery_Hour, 
                               Delivery_Interval = dataset.temp$Delivery_Interval, SMP_D_Euro = dataset.temp$SMP_D_Euro, 
                               SMP_D_Minus_6_Euro = dataset.temp$SMP_D_Minus_6_Euro, 
                               SMP_D_Minus_13_Euro = dataset.temp$SMP_D_Minus_13_Euro, 
                               SMP_D_Minus_7_HH_Minus_1_Euro = dataset.temp$SMP_HH_Minus_1_Euro, 
                               SMP_D_Minus_7_HH_Minus_2_Euro = dataset.temp$SMP_HH_Minus_2_Euro, 
                               Difference = dataset.temp$difference,
                               Power_Production_UK = dataset.temp$Power_Production_UK,
                               Output_SMP_Euro = dataset.temp$Output_SMP_Euro)
    
    dataset.train <- dataset.temp %>% filter(Report_Date >= start.train, 
                                             Report_Date <= end.train) %>% select(-Report_Date)
    dataset.test <- dataset.temp %>% filter(Report_Date >= start.test, 
                                            Report_Date <= end.test) %>% select(-Report_Date)
    
    varnames <- colnames(dataset.train)
    jsonVar <- serializeJSON(varnames)
    
    method <- list(Name = "None", Description = "Do Nothing")
    
    input <- list(SQL_Query = jsonVar, Training_Testing_Rate = rate, 
                  Input_Discription = Input_Discription, StartDate_Training = start.train, 
                  EndDate_Training = end.train, StartDate_Testing = start.test, 
                  EndDate_Testing = end.test)
    
    Input_Discription <- "Dataset of Weekly Value with difference between demand and power production but without UK Power Production: Dhh, D-7hh, D-7hh-1, D-7hh-2 D-14hh with UK"
    return(list(dataset.train = dataset.train, dataset.test = dataset.test, 
                input = input, preprocessing = method, Input_Discription = Input_Discription))
}

DatasetAquisition.WeeklyOfDayPlusHalfHourWithoutUK.UTC <- function(conn, 
   start.train = (Sys.Date() - 15), end.train = Sys.Date(), 
   start.test = (Sys.Date() - 15), end.test = Sys.Date()) {
  
    start <- min(c(start.train, start.test, end.train, end.test))
    end <- max(c(start.train, start.test, end.train, end.test))
    sqlquery <- paste("SELECT [Trade_Date],[delivery_date],[delivery_hour],[delivery_interval],[SMP] FROM [dbo].[WA_SEMO_MarketResults] where [delivery_date] between '", 
                      (start - 20), "' and '", (end + 1), "' order by delivery_date asc, delivery_hour asc, delivery_interval asc")
    dataset.smp <- sqlQuery(conn, sqlquery)
    dataset.smp[dataset.smp < 0] <- 0
    dataset.smp$delivery_date <- as.Date(as.character(dataset.smp$delivery_date))
    dataset.smp$Trade_Date <- as.Date(as.character(dataset.smp$Trade_Date))
    
    dataset.smp.lag7 <- dataset.smp
    dataset.smp.lag7$delivery_date <- dataset.smp$delivery_date + 
      6
    dataset.smp.lag7$Trade_Date <- dataset.smp$Trade_Date + 
      6
    
    dataset.smp.lag14 <- dataset.smp
    dataset.smp.lag14$delivery_date <- dataset.smp$delivery_date + 
      13
    dataset.smp.lag14$Trade_Date <- dataset.smp$Trade_Date + 
      13
    
    dataset.smp.hhlag1 <- dataset.smp[, c("Trade_Date", "delivery_date", 
                                          "delivery_hour", "delivery_interval")]
    dataset.smp.hhlag1$SMP_HH_Minus_1_Euro <- c(NA, dataset.smp$SMP[1:(nrow(dataset.smp) - 
                                                                         1)])
    
    dataset.smp.hhlag2 <- dataset.smp[, c("Trade_Date", "delivery_date", 
                                          "delivery_hour", "delivery_interval")]
    dataset.smp.hhlag2$SMP_HH_Minus_2_Euro <- c(NA, NA, dataset.smp$SMP[1:(nrow(dataset.smp) - 
                                                                             2)])
    
    dataset.smp.output <- dataset.smp
    dataset.smp.output$delivery_date <- dataset.smp$delivery_date - 
      1
    dataset.smp.output$Trade_Date <- dataset.smp$Trade_Date - 
      1
    
    smp.part <- inner_join(dataset.smp, dataset.smp.lag7, 
                           by = c("Trade_Date", "delivery_date", "delivery_hour", 
                                  "delivery_interval"))
    smp.part <- inner_join(smp.part, dataset.smp.lag14, by = c("Trade_Date", 
                                                               "delivery_date", "delivery_hour", "delivery_interval"))
    smp.part <- inner_join(smp.part, dataset.smp.hhlag1, 
                           by = c("Trade_Date", "delivery_date", "delivery_hour", 
                                  "delivery_interval"))
    smp.part <- inner_join(smp.part, dataset.smp.hhlag2, 
                           by = c("Trade_Date", "delivery_date", "delivery_hour", 
                                  "delivery_interval"))
    smp.part <- inner_join(smp.part, dataset.smp.output, 
                           by = c("Trade_Date", "delivery_date", "delivery_hour", 
                                  "delivery_interval"))
    
    smp.part$Trade_Date <- smp.part$Trade_Date - 1
    smp.part$delivery_date <- smp.part$delivery_date + 1
    colnames(smp.part) <- c("Report_Date", "Delivery_Date", 
                            "Delivery_Hour", "Delivery_Interval", "SMP_D_Euro", 
                            "SMP_D_Minus_6_Euro", "SMP_D_Minus_13_Euro", "SMP_HH_Minus_1_Euro", 
                            "SMP_HH_Minus_2_Euro", "Output_SMP_Euro")
    
    sqlquery.loadDemand <- paste("SELECT [REPORT_DATE],[TRADE_DATE],[JURISDICTION],[DELIVERY_DATE],[DELIVERY_HOUR],[DELIVERY_INTERVAL],[FORECAST_MW] FROM [dbo].[WA_SEMO_FourDayLoadForecast] where [report_date] between '", 
                                 start, "' and '", end, "' order by report_date asc, delivery_hour asc, delivery_interval asc")
    load.part <- sqlQuery(conn, sqlquery.loadDemand)
    
    load.part <- load.part %>% arrange(REPORT_DATE)
    load.part$REPORT_DATE <- as.Date(as.character(load.part$REPORT_DATE))
    load.part <- load.part %>% group_by(REPORT_DATE, DELIVERY_DATE, 
                                        DELIVERY_HOUR, DELIVERY_INTERVAL, JURISDICTION) %>% 
      summarise(loadDemand = first(FORECAST_MW))
    load.part <- load.part %>% group_by(REPORT_DATE, DELIVERY_DATE, 
                                        DELIVERY_HOUR, DELIVERY_INTERVAL) %>% summarise(loadDemand = sum(loadDemand))
    
    load.part$DELIVERY_DATE <- as.Date(as.character(load.part$DELIVERY_DATE))
    colnames(load.part) <- c("Report_Date", "Delivery_Date", 
                             "Delivery_Hour", "Delivery_Interval", "LoadDemand")
    
    sqlquery.power <- paste("SELECT [REPORT_DATE],[DELIVERY_DATE],[REGIONS],[SOLARPOWER_MW],[WINDPOWER_MW] FROM [dbo].[WA_Metagroup_RenewablePowerProduction] where [report_date] between '", 
                            start, "' and '", end, "' order by report_date asc")
    power.part <- sqlQuery(conn, sqlquery.power)
    
    power.part <- power.part %>% mutate(Power = SOLARPOWER_MW + 
                                          WINDPOWER_MW) %>% arrange(REPORT_DATE, DELIVERY_DATE, 
                                                                    REGIONS)
    power.part$REPORT_DATE <- as.Date(as.character(power.part$REPORT_DATE))
    
    power.part.ireland <- power.part %>% filter(REGIONS != 
                                                  "UK")
    power.part.ireland <- power.part.ireland %>% group_by(REPORT_DATE, 
                                                          DELIVERY_DATE) %>% summarise(Power = sum(Power)) %>% 
      separate(DELIVERY_DATE, c("DELIVERY_DATE", "DELIVERY_HOUR"), 
               sep = " ")
    power.part.ireland$DELIVERY_HOUR <- format(strptime(power.part.ireland$DELIVERY_HOUR, 
                                                        format = "%H:%M:%S"), format = "%H")
    power.part.ireland$DELIVERY_HOUR <- as.numeric(power.part.ireland$DELIVERY_HOUR) + 
      1
    power.part.ireland$DELIVERY_DATE <- as.Date(as.character(power.part.ireland$DELIVERY_DATE))
    colnames(power.part.ireland) <- c("Report_Date", "Delivery_Date", 
                                      "Delivery_Hour", "Power_Production_Ireland")
    
    dataset.temp <- inner_join(smp.part, load.part, by = c("Report_Date", 
                                                           "Delivery_Date", "Delivery_Hour", "Delivery_Interval"))
    dataset.temp <- inner_join(dataset.temp, power.part.ireland, 
                               by = c("Report_Date", "Delivery_Date", "Delivery_Hour"))
    
    dataset.temp <- data.frame(Report_Date = dataset.temp$Report_Date,Delivery_Date = dataset.temp$Delivery_Date, 
                                  Delivery_Hour = dataset.temp$Delivery_Hour, Delivery_Interval = dataset.temp$Delivery_Interval, 
                                  SMP_D_Euro = dataset.temp$SMP_D_Euro, SMP_D_Minus_6_Euro = dataset.temp$SMP_D_Minus_6_Euro, 
                                  SMP_D_Minus_13_Euro = dataset.temp$SMP_D_Minus_13_Euro, 
                                  SMP_HH_Minus_1_Euro = dataset.temp$SMP_HH_Minus_1_Euro, 
                                  SMP_HH_Minus_2_Euro = dataset.temp$SMP_HH_Minus_2_Euro, 
                                  LoadDemand = dataset.temp$LoadDemand, Power_Production_Ireland = dataset.temp$Power_Production_Ireland, 
                                  Output_SMP_Euro = dataset.temp$Output_SMP_Euro)
    
  
    dataset.train <- dataset.temp %>% filter(as.POSIXlt(as.character(Delivery_Date))$isdst == 1) %>% select(-Report_Date)
    dataset.test <- dataset.temp %>% filter(as.POSIXlt(as.character(Delivery_Date))$isdst == 0) %>% select(-Report_Date)
    
  varnames <- colnames(dataset.train)
  jsonVar <- serializeJSON(varnames)
  
  method <- list(Name = "None", Description = "Do Nothing")
  
  input <- list(SQL_Query = jsonVar, Training_Testing_Rate = rate, 
                Input_Discription = Input_Discription, StartDate_Training = start.train, 
                EndDate_Training = end.train, StartDate_Testing = start.test, 
                EndDate_Testing = end.test)
  
  Input_Discription <- "Dataset of Weekly Value without UK Power Production: Dhh, D-7hh, D-14hh, Dhh-1, Dhh-2 without UK and separate dataset into UTC and IST"
  
  return(list(dataset.train = dataset.train, dataset.test = dataset.test, 
              input = input, preprocessing = method, Input_Discription = Input_Discription))
}
