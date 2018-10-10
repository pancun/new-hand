setwd('/data/ChinaStocks/Transactions/FromSina/2004')
rm(list=ls())
library(snowfall)


file=dir()
error_report=data.frame(matrix(NA,1,1))
setnames(error_report,'wind_code')

sfInit(parallel = TRUE, cpus = 10)
sfLibrary(data.table)     
sfLibrary(tidyverse)  

data_transform=function(file_name)
{
  
  report=error_report
  
  dt = readr::read_csv(file_name,locale = locale(encoding = 'GB18030'),col_names = F) 
  
  tmp = lapply(1:dim(dt)[1],function(n) 
    setDT(
      transpose(
        strsplit(
          unlist(
            dt[n,]),"\t")
      )
    )
  )
  
  tmp=rbindlist(tmp)
  
  setnames(tmp,tolower(names(tmp)))
  
  error_num=dim(tmp[is.na(v6),])[1]
  
  if(error_num!=0)
  {
    report=mutate(error_report,wind_code=file_name)
    return(report)
  }
}

error_search=function(direction)
{
  file_name=dir(direction)
  file_name=paste0(direction,'/',file_name)
  temp_error_dt=lapply(file_name,data_transform)
  error_dt=rbindlist(temp_error_dt)
  error_dt=rbind(error_dt,error_report)
  error_dt=mutate(error_dt,date=direction)
  return(error_dt)
}

     
sfExport('data_transform','error_search')   
sfExport('error_report')

temp_error_dt=sfLapply(file[31:50],error_search)
error_dt=rbindlist(temp_error_dt)

sfStop()


# for (i in 3:5)
# {
#   temp=data_transform(file[i])
#   error_report=rbind(error_report,temp)
# }



  

