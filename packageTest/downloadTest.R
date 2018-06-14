resolveURI <- "https://cn-sandbox.test.dataone.org/cn/v2/resolve"
#d1c <- D1Client("SANDBOX", "urn:node:mnSandboxUCSB1")
d1c <- D1Client("DEV", "urn:node:mnDemo6")
#id <- "urn:uuid:27cdd41f-8e77-4508-b229-226a49eca224"
# on mn-demo-6
id <- "urn:uuid:175a5ee6-9f14-4bf7-bf89-4fd5e535870b"

pkg <- getDataPackage(d1c, identifier=id, lazyLoad=TRUE, limit="0GB", quiet=FALSE)
