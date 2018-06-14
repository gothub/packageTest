library(datapack)
dp <- new("DataPackage")

# This DataObject contains the program script that was executed
progObj <- new("DataObject", format="application/R", 
               filename=system.file("extdata/pkg-example/logit-regression-example.R", package="datapack"),
               suggestedFilename="logit-regression-example.R")
dp <- addMember(dp, progObj)

doIn <- new("DataObject", format="text/csv", 
            filename=system.file("./extdata/pkg-example/binary.csv.zip", package="datapack"),
            suggestedFilename="binary.csv")
dp <- addMember(dp, doIn)

doOut <- new("DataObject", format="image/png", 
             filename=system.file("./extdata/pkg-example/gre-predicted.png", package="datapack"),
             suggestedFilename="gre-predicted.png")
dp <- addMember(dp, doOut)

# Test the replaceMember functionality
dp <- replaceMember(dp, doIn, replacement=system.file("./extdata/pkg-example/binary.csv", 
                                                               format="application/octet-stream"))

# The arguments "sources" and "derivations" can also contain lists of "DataObjects"
dp <- describeWorkflow(dp, sources=doIn, program=progObj, derivations=doOut)
rels <- getRelationships(dp, condense=TRUE)
rels[grepl("prov:", rels$predicate),]
