library(dataone)
library(datapack)
library(xml2)
library(digest)
library(uuid)
# Create a csv file for the science object
d1c <- D1Client("STAGING2", "urn:node:mnTestKNB")

# Download the original package and just add provenance relationships
# This should cause the resource map object and pid to be updated only. The resource map
# pid should contain an increment number, since the metadata pid was used for the original
resmapId <- "resource_map_urn:uuid:07e68357-d528-497d-b6de-76fb92fb049b"
pkg <- getDataPackage(d1c, id=resmapId, lazyLoad=TRUE, limit="0MB", quiet=FALSE)

sourceData <- system.file("extdata/OwlNightj.csv", package="dataone")
outputData <- system.file("extdata/Strix-occidentalis-obs.csv", package="dataone")

sourceObjId <- selectMember(pkg, name="sysmeta@fileName", value=basename(sourceData))
programObjId <- selectMember(pkg, name="sysmeta@formatId", value="application/R")
outputObjId <- selectMember(pkg, name="sysmeta@fileName", value=basename(outputData))

pkg <- describeWorkflow(pkg, sources=sourceObjId, program=programObjId, derivations=outputObjId)
resmapId <- uploadDataPackage(d1c, pkg, public=TRUE, quiet=FALSE)
