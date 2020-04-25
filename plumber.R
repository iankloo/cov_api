library(plumber)
library(DBI)
library(RSQLite)
library(dplyr)
library(dbplyr)

#* @apiTitle COVID Data API

#' serve the data from the columns selected by the user
#' @param min_date
#' @param max_date
#' @param county_fips
#' @get /alldata
#' @json

function(min_date = NULL, max_date = NULL, county_fips = NULL){
  covid_db <- dbConnect(RSQLite::SQLite(), 'data/covid_db.sqlite')
  df <- tbl(covid_db, 'counties')
  
  if(!is.null(county_fips)){
    county_fips <- unlist(strsplit(county_fips, ','))
    df <- df %>%
      filter(countyFIPS %in% county_fips)
  }
  
  #create text dates to pull (stored as text in DB)
  if(is.null(min_date)){
    if(is.null(max_date)){
      dat <- as.character(Sys.Date() - 1)
      
      x <- df %>%
        filter(date == dat) %>%
        collect()
      
      return(x)
    } else{
      return('Must provide min_date if you provide max_date.')
    }
  } else{
    if(is.null(max_date)){
      return('Must provide max_date if you provide min_date.')
    } else{
      
      max <- as.Date(max_date, format = '%Y%m%d')
      min <- as.Date(min_date, format = '%Y%m%d')
      
      if(is.na(max) | is.na(min)){
        return('Please provide the dates in yyyymmdd format.')
      }
      
      if(max < min){
        return('max_date cannot be after min_date')
      }else{
        date_seq <- as.character(seq(min, max, by = 1))
        
        x <- df %>%
          filter(date %in% date_seq) %>%
          collect()
        
        return(x)
      }
    }
  }
}


#' serve geojson
#' @get /bigmap
#' @json

function(){
  geojsonio::geojson_read('data/ts.geojson')
}
