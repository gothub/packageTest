library(dataone)
library(datapack)
library(uuid)
library(hash)

#d1cDev <- D1Client("STAGING2", "urn:node:mnTestKNB")
#d1cDev <- D1Client("STAGING2", "urn:node:mnTestKNB")
#d1cDev <- D1Client("DEV", "urn:node:mnDemo6")
dp <- new("DataPackage")

# This DataObject contains the program script that was executed
progObj <- new("DataObject", format="application/R", 
               filename=system.file("extdata/pkg-example/logit-regression-example.R", package="datapack"),
               suggestedFilename="logit-regression-example.R")
dp <- addMember(dp, progObj)

doIn <- new("DataObject", format="text/csv", 
            filename=system.file("./extdata/pkg-example/binary.csv", package="datapack"),
            suggestedFilename="binary.csv")

dp <- addMember(dp, doIn)

doOut <- new("DataObject", format="image/png", 
             filename=system.file("./extdata/pkg-example/gre-predicted.png", package="datapack"),
             suggestedFilename="gre-predicted.png")
dp <- addMember(dp, doOut)

# Test the replaceMember functionality
dp <- replaceMember(dp, doIn, replacement=system.file("./extdata/pkg-example/binary.csv.zip", package="datapack"),
                    format="application/octet-stream", suggestedFilename="binary.csv.zip")

# The arguments "sources" and "derivations" can also contain lists of "DataObjects"
dp <- describeWorkflow(dp, sources=doIn, program=progObj, derivations=doOut)
rels <- getRelationships(dp, condense=TRUE)
show(rels)

# This call should fail for this new package
#updateId <- updateDataPackage(d1cDev, dp, public=TRUE, quiet=FALSE)

#for (thisDate in dates) {
#  cat(sprintf("this date = %s\n", thisDate))
#  #if(!is.na(thisDate)) {
#  #    stop(sprintf("The DataObject with identifier %s has already been uploaded to a respository.\nPlease use 'updateDataPackage' instead"), thisId)
#  #}
#}
