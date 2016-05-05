getTeamMemberDF <- function(teamId) {
  userListREST <- synRestGET(sprintf("/teamMembers/%s?limit=500", teamId))
  userList <- ldply(userListREST$results,
                    function(x) data.frame(userId=as.character(x$member$ownerId), 
                                           teamId=as.character(x$teamId)))
  userList
}

aclToUserList <- function(id) {
  acl <- synGetEntityACL(id)
  
  aclUserList <- ldply(acl@resourceAccess@content, 
                       function(x) data.frame(principalId=as.character(x@principalId),
                                              teamId="syn2862345"))
  
  
  userList <- ldply(aclUserList$principalId, getTeamMemberDF)
  
  userList2 <- aclUserList %>% 
    filter(!(principalId %in% userList$userId)) %>% 
    dplyr::rename(userId=principalId)
  
  rbind(userList, userList2)
  
}

getDownloads <- function(projectId, beginTimestamp, endTimestamp) {
  q.downloads <- sprintf('select CLIENT,NORMALIZED_METHOD_SIGNATURE,PROJECT_ID,BENEFACTOR_ID,PARENT_ID,ENTITY_ID,AR.TIMESTAMP,RESPONSE_STATUS,DATE,USER_ID,NODE_TYPE,N.NAME from ACCESS_RECORD AR, PROCESSED_ACCESS_RECORD PAR, NODE_SNAPSHOT N, (select distinct ID from NODE_SNAPSHOT where PROJECT_ID = "%s") NODE where AR.TIMESTAMP Between %s AND %s and AR.SESSION_ID = PAR.SESSION_ID and AR.TIMESTAMP = PAR.TIMESTAMP and PAR.ENTITY_ID = NODE.ID and N.ID = NODE.ID and (PAR.NORMALIZED_METHOD_SIGNATURE = "GET /entity/#/file" or PAR.NORMALIZED_METHOD_SIGNATURE = "GET /entity/#/version/#/file");', projectId, beginTimestamp, endTimestamp)
  dbGetQuery(conn = con, statement=q.downloads) %>% 
    dplyr::rename(userid=USER_ID, id=ENTITY_ID)
}

getWebAccess <- function(projectId, beginTimestamp, endTimestamp) {
  q.browse <- sprintf('select NORMALIZED_METHOD_SIGNATURE,PROJECT_ID,BENEFACTOR_ID,PARENT_ID,ENTITY_ID,CONVERT(AR.TIMESTAMP, CHAR) AS TIMESTAMP,RESPONSE_STATUS,DATE,USER_ID,NODE_TYPE,N.NAME from ACCESS_RECORD AR, PROCESSED_ACCESS_RECORD PAR, NODE_SNAPSHOT N, (select distinct ID from NODE_SNAPSHOT where PROJECT_ID = "%s") NODE where AR.TIMESTAMP Between %s AND %s and AR.SESSION_ID = PAR.SESSION_ID and AR.TIMESTAMP = PAR.TIMESTAMP and PAR.ENTITY_ID = NODE.ID and N.ID = NODE.ID and CLIENT = "WEB" AND (PAR.NORMALIZED_METHOD_SIGNATURE = "GET /entity/#/bundle" OR PAR.NORMALIZED_METHOD_SIGNATURE = "GET /entity/#/version/#/bundle" OR PAR.NORMALIZED_METHOD_SIGNATURE = "GET /entity/#/wiki2" OR PAR.NORMALIZED_METHOD_SIGNATURE = "GET /entity/#/wiki2/#");', projectId, beginTimestamp, endTimestamp)
  dbGetQuery(conn = con, statement=q.browse) %>% 
    dplyr::rename(userid=USER_ID, id=ENTITY_ID)
}
