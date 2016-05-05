config <- yaml.load_file("mysql_config.yml")

projectId <- '2862345'

con <- dbConnect(MySQL(),
                 user = config$username,
                 password = config$password,
                 host = config$host,
                 dbname='warehouse')

nMonths <- 6

endDate <- as.POSIXct(Sys.Date(), origin="1970-01-01", tz="PST")
endTimestamp <- as.numeric(endDate) * 1000

dateRanges <- ldply(seq(nMonths, 0, by=-1),
                    function(x) {
                      data.frame(number=x,
                                 begin=endDate - months(x + 1),
                                 end=endDate - months(x)) %>% 
                        mutate(beginTimestamp=as.numeric(begin) * 1000,
                               endTimestamp=as.numeric(end) * 1000)
                    })

doQuery <- function(projectId, beginTimestamp, endTimestamp) {
  q.downloads <- sprintf('select CLIENT,NORMALIZED_METHOD_SIGNATURE,PROJECT_ID,BENEFACTOR_ID,PARENT_ID,ENTITY_ID,AR.TIMESTAMP,RESPONSE_STATUS,DATE,USER_ID,NODE_TYPE,N.NAME from ACCESS_RECORD AR, PROCESSED_ACCESS_RECORD PAR, NODE_SNAPSHOT N, (select distinct ID from NODE_SNAPSHOT where PROJECT_ID = "%s") NODE where AR.TIMESTAMP Between %s AND %s and AR.SESSION_ID = PAR.SESSION_ID and AR.TIMESTAMP = PAR.TIMESTAMP and PAR.ENTITY_ID = NODE.ID and N.ID = NODE.ID and (PAR.NORMALIZED_METHOD_SIGNATURE = "GET /entity/#/file" or PAR.NORMALIZED_METHOD_SIGNATURE = "GET /entity/#/version/#/file");', projectId, beginTimestamp, endTimestamp)
  
  dbGetQuery(conn = con, statement=q.downloads) %>% 
    dplyr::rename(userid=USER_ID, id=ENTITY_ID)
}

res <- ddply(dateRanges, .(number), 
             function(x) {doQuery(projectId, 
                                  x$beginTimestamp, 
                                  x$endTimestamp)})

foo <- res %>% filter(RESPONSE_STATUS == 200)  %>% count(userid, id, DATE, TIMESTAMP, NODE_TYPE) %>% ungroup() %>% 
  mutate(id=as.character(id))
