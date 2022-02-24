
library(aws.s3)
library(arrow)
library(epidatr)
library(tidyverse)
parity <- function(event) {
  
  bucket_name <- Sys.getenv("bucket_name")
  forecast_loc <- Sys.getenv("focecast_loc")
  forecast_date <- Sys.Date()

  abb <- covidHubUtils::hub_locations %>% 
    filter(fips == forecast_loc) %>% 
    pull(abbreviation)
  issue_date <- lubridate::ymd(forecast_date)
  out <- tibble()

  while (issue_date > "2020-11-02"){ # release date on healthdata.gov is 2020-11-03, although earliest issue currently available seems to be 11/16
    issue <- issue_date %>% stringr::str_remove_all("-")
    dates <- paste0("20200101-", issue)
    req <- try(healthdata(dates = dates, states = abb, issues = issue))
    if (inherits(req, "try-error")){
      issue_date <- issue_date - lubridate::ddays(1)
    } else {
      out <- req$epidata
      if (nrow(out) == 1) { ## daily issue, keep looking for issue with all dates
        issue_date <- issue_date - lubridate::ddays(1)
      } else{
        break
      }
    }
  }

  outdir <- file.path("healthdata", forecast_date) 
  file_name = file.path(outdir, "epidata.parquet")
  #file_name <- "epi.parquet" 
  writeToS3(out, bucket_name, file_name)

 
}

 writeToS3 <- function(file,bucket,filename){
    s3write_using(file, FUN = write_parquet,
                  bucket = bucket,
                  object = filename)
  }
