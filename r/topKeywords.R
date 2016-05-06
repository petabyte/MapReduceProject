multiplot <- function(..., plotlist=NULL, file, cols=1, layout=NULL) {
  library(grid)
  
  # Make a list from the ... arguments and plotlist
  plots <- c(list(...), plotlist)
  
  numPlots = length(plots)
  
  # If layout is NULL, then use 'cols' to determine layout
  if (is.null(layout)) {
    # Make the panel
    # ncol: Number of columns of plots
    # nrow: Number of rows needed, calculated from # of cols
    layout <- matrix(seq(1, cols * ceiling(numPlots/cols)),
                     ncol = cols, nrow = ceiling(numPlots/cols))
  }
  
  if (numPlots==1) {
    print(plots[[1]])
    
  } else {
    # Set up the page
    grid.newpage()
    pushViewport(viewport(layout = grid.layout(nrow(layout), ncol(layout))))
    
    # Make each plot, in the correct location
    for (i in 1:numPlots) {
      # Get the i,j matrix positions of the regions that contain this subplot
      matchidx <- as.data.frame(which(layout == i, arr.ind = TRUE))
      
      print(plots[[i]], vp = viewport(layout.pos.row = matchidx$row,
                                      layout.pos.col = matchidx$col))
    }
  }
}

  dfLogDataTaxSeason <- read.csv("top30sortedTaxSeasonKeywords.txt", sep="\t",stringsAsFactors=F)
  dfLogData <- read.csv("top30sortedTopKeywords.txt", sep="\t",stringsAsFactors=F)
  dfAnalysis <- read.csv("top30analysisKeywords.txt",sep="\t",stringsAsFactors=F) 
  colnames (dfLogData) <- c("noOfSearches", "searchKeyWord")
  colnames (dfLogDataTaxSeason) <- c("noOfSearches", "searchKeyWord")
  colnames (dfAnalysis) <- c("chiValue", "searchKeyWord")
  
  
  p1 <- ggplot(data=dfLogDataTaxSeason,aes(x=searchKeyWord,y=noOfSearches,fill=noOfSearches)) +
    geom_bar(colour="red",stat = "identity")+
    ggtitle("Most Searched Keywords During Tax Season")
    
  p2 <-  ggplot(data=dfLogData,aes(x=searchKeyWord,y=noOfSearches,fill=noOfSearches)) + geom_bar( stat = "identity") +
    ggtitle("Most Searched Keywords All Time")
  
  p3 <- ggplot(data=dfAnalysis,aes(x=searchKeyWord,y=chiValue,group=1)) + geom_line(colour="blue") +
    ggtitle("Comparative Analysis Tax Season vs All Time")
  
  multiplot(p1, p2, p3, cols=2)