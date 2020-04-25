#---improved munge script for covid data
library(data.table)
library(RSQLite)
library(DBI)
library(dplyr)
library(dbplyr)
library(mixdist)
library(extraDistr)

#functions used
rt.func.v2<-function(dat,mean.Weibull=4.8,sd.Weibull=2.3){
  r.vals<-numeric(length = (length(dat) - 2))
  #get the Weibull parameters from mixdist's weibullpar function
  mGT.params<-weibullpar(mean.Weibull, sd.Weibull, loc = 0)
  alpha<-mGT.params[2] # called shape in weibullpar, alpha in a discrete Weilbull
  beta<-mGT.params[1] # called scale in weibullpar, beta in a discrete Weibull
  #the extraDistr package uses an altrnative parameterization of the Weibull (q, beta) from
  #Nakagawa and Osaki (1975) where q = exp(-alpha^-beta), so...
  q<-exp(-as.numeric(alpha)^(-as.numeric(beta)))
  #Discretize Weibull via the extraDistr package's ddweibull function
  w<- ddweibull(0:1000, as.numeric(q), as.numeric(beta), log = FALSE)
  growth<-diff(dat)
  growth<-pmax(growth, 0) # eliminate any erroneous downward shifts in the cumulative counts
  #Estimate R(t) from equation (33) of Nishiura and Chowell (2009)
  for(k in 2:length(growth)){
    r.vals[k-1]<-growth[k]/(sum(growth[1:k]*rev(w[1:k])))
  }
  #Output the results
  return(c(NA, NA, r.vals))
}



#setwd('~/working/cov_map_working/')


covid_db <- dbConnect(RSQLite::SQLite(), 'data/covid_db.sqlite')

#---check if need to update
x <- tbl(covid_db, 'counties') %>%
  filter(date == max(date, na.rm = TRUE)) %>%
  select(date) %>%
  collect()

db_date <- as.Date(x[1][[1]][1])


main <- fread('https://usafactsstatic.blob.core.windows.net/public/data/covid-19/covid_confirmed_usafacts.csv', colClasses = 'character')
main <- main[countyFIPS != '0']
keep_cols <- grep('V',colnames(main), invert = TRUE)
main <- main[, keep_cols, with=FALSE]

facts_date <- as.Date(colnames(main)[length(colnames(main))], format = '%m/%d/%y')

#if newer date in usafacts than db
if(db_date != facts_date){
  
  #---finish cleaning up the usafacts data
  main[nchar(countyFIPS) == 4, 'countyFIPS'] <- paste0('0', main[nchar(countyFIPS) == 4, countyFIPS])
  
  df <- melt.data.table(main, id.vars = 1:4, measure.vars = 5:ncol(main), variable.name = 'date', value.name = 'case_count')
  df[, date := as.Date(as.character(date), format = '%m/%d/%y')][
    , case_count := as.integer(gsub(',', '', case_count))
    ]
  
  #add daily growth
  df[, delta := lapply(.SD, function(d) d - shift(d)), by = countyFIPS, .SDcols = 'case_count']
  
  #add percent increase
  df[, per_delta := lapply(.SD, function(d) (d - shift(d))/shift(d)), by = countyFIPS, .SDcols = 'case_count']
  df[is.nan(per_delta), 'per_delta'] <- NA
  df[is.infinite(per_delta), 'per_delta'] <- NA
  
  #add rt
  df[, r_t := rt.func.v2(case_count), by = 'countyFIPS']
  
  #brind in deaths
  deaths <- fread('https://usafactsstatic.blob.core.windows.net/public/data/covid-19/covid_deaths_usafacts.csv',
                  colClasses = 'character')
  
  #drop bad data
  d <- deaths[duplicated(deaths) == FALSE]
  d <- d[countyFIPS != '0']
  keep_cols <- grep('V',colnames(d), invert = TRUE)
  d <- d[, keep_cols, with=FALSE]
  
  #fix fips codes with leading 0
  d[nchar(countyFIPS) == 4, 'countyFIPS'] <- paste0('0', d[nchar(countyFIPS) == 4, countyFIPS])
  
  d <- melt.data.table(d, id.vars = 1:4, measure.vars = 5:ncol(d), variable.name = 'date', value.name = 'deaths')
  d[, date := as.Date(as.character(date), format = '%m/%d/%y')][
    , deaths := as.integer(gsub(',', '', deaths))
    ]
  
  d <- d[, c('countyFIPS', 'date', 'deaths')]
  setkeyv(d, c('countyFIPS', 'date'))
  setkeyv(df, c('countyFIPS', 'date'))
  
  #merge deaths
  df <- d[df]
  
  #--add pops
  pop <- fread('https://www2.census.gov/programs-surveys/popest/datasets/2010-2019/counties/totals/co-est2019-alldata.csv',
               colClasses = 'character')
  
  pop[, countyFIPS := paste0(STATE, COUNTY)]
  pop <- pop[, c('countyFIPS', 'POPESTIMATE2019')]
  pop[, pop := as.numeric(POPESTIMATE2019)]
  pop <- pop[, c('countyFIPS', 'pop')]
  
  setkey(pop, countyFIPS)
  setkey(df, countyFIPS)
  
  df <- pop[df]
  
  #---per capita cases and deaths
  df[, cases_per_10k := (case_count / pop) * 10000]
  df[, deaths_per_10k := (deaths / pop) * 10000]
  
  #---moving averages
  df <- df[order(countyFIPS, date)]
  df <- df[, r_t_three := frollmean(r_t, n = 3), by = countyFIPS]
  
  #---check if day before agrees with data as a check...
  yesterday <- as.character(max(df$date) - 1)
  df[, date := as.character(date)]
  
  x <- tbl(covid_db, 'counties') %>%
    filter(date == yesterday, countyFIPS == '01001') %>%
    select(r_t) %>%
    collect()
  
  good <- x$r_t == df[date == yesterday & countyFIPS == '01001', r_t]
  
  if(good){
    dbWriteTable(covid_db, 'counties', df, overwrite = TRUE)
    
    #build geojson file
    dat <- tbl(covid_db, 'counties') %>%
      select(countyFIPS, date, case_count, delta, per_delta, r_t, deaths, cases_per_10k, r_t_three) %>%
      distinct() %>%
      collect()
    
    dat <- data.table(dat)
    dat <- dat[as.Date(dat$date) >= '2020-03-01',]
    
    dat[, r_t := round(r_t, 2)]
    dat[, r_t_three := round(r_t_three, 2)]
    dat[, per_delta := round(per_delta* 100, 2) ]
    
    #---make wide timeseries data - every variable/date combo gets a column
    u_id <- unique(dat$countyFIPS)
    out <- list()
    pb <- txtProgressBar(max = length(u_id), style = 3)
    for(i in 1:length(u_id)){
      sub <- dat[countyFIPS == u_id[i]]
      sub <- unique(sub, by=c("countyFIPS", "date"))
      
      out_tmp <- list()
      for(j in 1:nrow(sub)){
        cols <- paste0(colnames(sub)[3:ncol(sub)],'_', gsub('-', '', sub$date[j]))
        tmp <- data.frame(sub[j, 3:ncol(sub)])
        colnames(tmp) <- cols
        out_tmp[[j]] <- tmp
      }
      z <- cbind(sub[1, 1], do.call('cbind', out_tmp))
      out[[i]] <- z
      setTxtProgressBar(pb, i)
    }
    
    final <- rbindlist(out)
    
    #merge into county shapes
    county_shapes <- readRDS('data/all_counties.RDS')
    
    rn <- row.names(county_shapes@data)
    
    county_shapes$STATE <- as.character(county_shapes$STATE)
    county_shapes$COUNTY <- as.character(county_shapes$COUNTY)
    county_shapes$FIPS <- paste0(county_shapes$STATE, county_shapes$COUNTY)
    
    county_shapes <- sp::merge(county_shapes, final, by.x = 'FIPS', by.y = 'countyFIPS')
    row.names(county_shapes) <- rn
    
    geojsonio::geojson_write(county_shapes, file = "data/ts.geojson")
  }
}




