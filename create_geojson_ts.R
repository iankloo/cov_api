#---build timeseries geojson for map

library(data.table)
library(jsonlite)
library(geojsonio)
library(sp)
library(RSQLite)
library(DBI)
library(dbplyr)
library(dplyr)

covid_db <- dbConnect(RSQLite::SQLite(), '/home/ubuntu/cov_api/data/covid_db.sqlite')

dat <- tbl(covid_db, 'counties') %>%
  select(countyFIPS, date, case_count, delta, per_delta, r_t, deaths, cases_per_10k, doubling, r_t_three) %>%
  distinct() %>%
  collect()

dat <- data.table(dat)
dat <- dat[as.Date(dat$date) >= '2020-03-01',]

# dat <- jsonlite::fromJSON(paste0('http://160.1.89.242/alldata?min_date=20200301&max_date=', gsub('-', '', Sys.Date() - 1)))
# dat <- data.table(dat)

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
county_shapes <- readRDS('~/working/cov_api/data/all_counties.RDS')

rn <- row.names(county_shapes@data)

county_shapes$STATE <- as.character(county_shapes$STATE)
county_shapes$COUNTY <- as.character(county_shapes$COUNTY)
county_shapes$FIPS <- paste0(county_shapes$STATE, county_shapes$COUNTY)

county_shapes <- sp::merge(county_shapes, final, by.x = 'FIPS', by.y = 'countyFIPS')
row.names(county_shapes) <- rn

geojsonio::geojson_write(county_shapes, file = "~/working/bigmap/ts.geojson")
#servr::httw('~/working/bigmap/', port = '8000', daemon = FALSE)

setwd('~/working/bigmap')
system('git add --all')
system('git commit -m "update"')
system('git push')




