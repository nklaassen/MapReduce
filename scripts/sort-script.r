#!/usr/bin/env Rscript

options(scipen=999)
library(ggplot2)
library(scales)
library(reshape2)
library(plyr)

data_summary <- function(data, varname, groupnames){
  summary_func <- function(x, col){
    c(mean = mean(x[[col]], na.rm=TRUE),
      sd = sd(x[[col]], na.rm=TRUE))
  }
  data_sum<-ddply(data, groupnames, .fun=summary_func,
                  varname)
  data_sum <- rename(data_sum, c("mean" = varname))
 return(data_sum)
}

data <- read.csv(file="sort-data.csv", header=TRUE, sep=",")
data <- melt(data, "array.size", variable.name="algorithm", value.name="time")
data$time <- data$time / 1000
data <- data_summary(data, varname="time", groupnames=c("algorithm", "array.size"))
data$array.size <- as.factor(data$array.size)

ggplot(data, aes(x=array.size, y=time, fill=algorithm)) +
    geom_col(color="black", position=position_dodge(.8)) +
    geom_errorbar(aes(ymin=time-sd, ymax=time+sd), width=.2, position=position_dodge(.8)) +
    labs(title="Sorting Algorithm Performance", y="time (ms)", x="array size")
ggsave("sort-timing.png")
