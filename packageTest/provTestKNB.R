#accessRules <- data.frame(subject=c("public"), permission=c("changePermission"))
#pkg <- removeAccessRule(pkg, accessRules)


resmapId <- 'resource_map_urn:uuid:216759e1-8d13-49da-a7b4-8a6686a2c161'

d1c <- D1Client("PROD", "urn:node:KNB")
pkg <- getDataPackage(d1c, id=resmapId, lazyLoad=TRUE, limit="0MB", quiet=FALSE)

programObjId <- selectMember(pkg, name="sysmeta@formatId", value='application/R')
outputObjId <- selectMember(pkg, name="sysmeta@formatId", value='text/csv')


pkg <- describeWorkflow(pkg, program=programObjId, derivations=outputObjId)
pkg